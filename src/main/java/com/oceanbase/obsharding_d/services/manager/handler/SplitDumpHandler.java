/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.store.fs.FileUtils;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.dump.*;
import com.oceanbase.obsharding_d.services.manager.response.DumpFileError;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.ExecutorUtil;
import com.oceanbase.obsharding_d.util.NameableExecutor;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SplitDumpHandler {

    private static final Pattern SPLIT_STMT = Pattern.compile("([^\\s]+)\\s+([^\\s]+)(((\\s*(-s([^\\s]+))?)|(\\s+(-r(\\d+))?)|(\\s+(-w(\\d+))?)|(\\s+(-l(\\d+))?)|(\\s+(--ignore)?)|(\\s+(-t(\\d+))?)|(\\s+(-b(\\d+))?))+)", Pattern.CASE_INSENSITIVE);
    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");


    private SplitDumpHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        LOGGER.info("begin to split dump file.");
        DumpFileConfig config = parseOption(stmt.substring(offset).trim());
        if (config == null) {
            LOGGER.info("split syntax is error.");
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax");
            return;
        }

        SchemaConfig defaultSchemaConfig = null;
        if (config.getDefaultSchema() != null) {
            defaultSchemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(config.getDefaultSchema());
            if (defaultSchemaConfig == null) {
                String errMsg = "Default schema[" + config.getDefaultSchema() + "] doesn't exist in config.";
                LOGGER.info(errMsg);
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, errMsg);
                return;
            }
        }


        BlockingQueue<String> deque = new ArrayBlockingQueue<>(config.getReadQueueSize());
        BlockingQueue<String> insertDeque = new ArrayBlockingQueue<>(config.getReadQueueSize());
        BlockingQueue<String> handleQueue = new ArrayBlockingQueue<>(config.getReadQueueSize());

        NameableExecutor fileExecutor = ExecutorUtil.createFixed("splitWorker", config.getThreadNum());
        NameableExecutor fileHandlerExecutor = ExecutorUtil.createFixed("splitHandler", 1);
        NameableExecutor fileReadExecutor = ExecutorUtil.createFixed("splitReader", 1);

        Map<String, String> errorMap = Maps.newConcurrentMap();
        AtomicBoolean errorFlag = new AtomicBoolean(false);
        //thread
        DumpFileReader reader = new DumpFileReader(handleQueue, errorMap, errorFlag);
        DumpFileHandler fileHandler = new DumpFileHandler(deque, insertDeque, handleQueue, fileExecutor, errorMap, errorFlag);
        DumpFileWriter writer = new DumpFileWriter(errorMap);
        DumpFileExecutor dumpFileExecutor = new DumpFileExecutor(deque, insertDeque, writer, config, defaultSchemaConfig, fileExecutor, errorFlag);
        try {
            reader.open(config.getReadFile(), config);
            writer.open(config.getWritePath() + FileUtils.getName(config.getReadFile()), config.getWriteQueueSize(), config.getMaxValues());

            // start write
            writer.start();
            // thread for process statement
            fileExecutor.execute(dumpFileExecutor);
            // firstly check file
            fileHandlerExecutor.execute(fileHandler);
            // start read
            fileReadExecutor.execute(reader);
        } catch (IOException e) {
            LOGGER.info("finish to split dump file because " + e.getMessage());
            service.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, e.getMessage());
            return;
        }

        List<ErrorMsg> errors = dumpFileExecutor.getContext().getErrors();
        if (!CollectionUtil.isEmpty(errors)) {
            recycleThread(fileReadExecutor, fileHandlerExecutor, fileExecutor, writer, true, errorMap);
            DumpFileError.execute(service, errors);
            return;
        }

        while (!service.getConnection().isClosed() && !writer.isFinished() && !errorFlag.get()) {
            LockSupport.parkNanos(1000);
        }
        //recycling thread
        recycleThread(fileReadExecutor, fileHandlerExecutor, fileExecutor, writer, errorFlag.get(), errorMap);

        errorMap.forEach((key, value) -> errors.add(new ErrorMsg(key, value)));

        if (service.getConnection().isClosed()) {
            LOGGER.info("finish to split dump file because the connection is closed.");
            //recycling thread
            recycleThread(fileReadExecutor, fileHandlerExecutor, fileExecutor, writer, errorFlag.get(), errorMap);
            service.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, "finish to split dump file due to the connection is closed.");
            return;
        }

        printMsg(errors, service);
        LOGGER.info("finish to split dump file.");
    }

    private static void recycleThread(NameableExecutor fileReadExecutor, NameableExecutor fileHandlerExecutor, NameableExecutor fileExecutor,
                                      DumpFileWriter writer, boolean errorFlag, Map<String, String> errorMap) {
        //recycling thread
        fileReadExecutor.shutdownNow();
        fileHandlerExecutor.shutdownNow();
        fileExecutor.shutdownNow();
        try {
            writer.stop(errorFlag);
        } catch (IOException e) {
            LOGGER.warn("error,because:", e);
            errorMap.putIfAbsent("error", "dump file error,because:" + e.getMessage());
        }
    }


    private static void printMsg(List<ErrorMsg> errors, ManagerService service) {
        if (CollectionUtil.isEmpty(errors)) {
            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.setMessage(StringUtil.encode("please see detail in dump.log.", service.getCharset().getResults()));
            packet.write(service.getConnection());
        } else {
            DumpFileError.execute(service, errors);
        }
    }

    private static DumpFileConfig parseOption(String options) {
        Matcher m = SPLIT_STMT.matcher(options);
        DumpFileConfig config = null;
        if (m.matches()) {
            config = new DumpFileConfig();
            config.setReadFile(m.group(1));
            config.setWritePath(m.group(2));
            if (m.group(7) != null) {
                config.setDefaultSchema(m.group(7));
            }
            if (m.group(10) != null) {
                config.setReadQueueSize(Integer.parseInt(m.group(10)));
            }
            if (m.group(13) != null) {
                config.setWriteQueueSize(Integer.parseInt(m.group(13)));
            }
            if (m.group(16) != null) {
                config.setMaxValues(Integer.parseInt(m.group(16)));
            }
            if (m.group(17) != null) {
                config.setIgnore(true);
            }
            if (m.group(21) != null) {
                config.setThreadNum(Integer.parseInt(m.group(21)));
            }
            if (m.group(24) != null) {
                config.setBufferSize(Integer.parseInt(m.group(24)));
            }
        }
        return config;
    }

}
