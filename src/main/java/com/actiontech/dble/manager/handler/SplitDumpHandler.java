package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.dump.*;
import com.actiontech.dble.manager.response.DumpFileError;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SplitDumpHandler {

    private static final Pattern SPLIT_STMT = Pattern.compile("([^\\s]+)\\s+([^\\s]+)\\s*(-s([^\\s]+))?\\s*(-r(\\d+))?\\s*(-w(\\d+))?\\s*(-l(\\d+))?", Pattern.CASE_INSENSITIVE);
    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");

    private SplitDumpHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        LOGGER.info("begin to split dump file.");
        DumpFileConfig config = parseOption(stmt.substring(offset).trim());
        if (config == null) {
            LOGGER.info("split syntax is error.");
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax");
            return;
        }

        SchemaConfig defaultSchemaConfig = null;
        if (config.getDefaultSchema() != null) {
            defaultSchemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(config.getDefaultSchema());
            if (defaultSchemaConfig == null) {
                String errMsg = "Default schema[" + config.getDefaultSchema() + "] doesn't exist in config.";
                LOGGER.info(errMsg);
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, errMsg);
                return;
            }
        }

        DumpFileWriter writer = new DumpFileWriter();
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(config.getReadQueueSize());
        DumpFileReader reader = new DumpFileReader(queue);
        DumpFileExecutor dumpFileExecutor = new DumpFileExecutor(queue, writer, config, defaultSchemaConfig);
        try {
            // firstly check file
            reader.open(config.getReadFile());
            String fileName = FileUtils.getName(config.getReadFile());
            writer.open(config.getWritePath() + fileName, config.getWriteQueueSize());

            // thread for process statement
            dumpFileExecutor.start();
            // start write
            writer.start();
            // start read
            reader.start(c, dumpFileExecutor);
        } catch (IOException | InterruptedException e) {
            LOGGER.info("finish to split dump file because " + e.getMessage());
            c.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, e.getMessage());
            return;
        }

        List<ErrorMsg> errors = dumpFileExecutor.getContext().getErrors();
        if (!CollectionUtil.isEmpty(errors)) {
            DumpFileError.execute(c, errors);
            return;
        }

        while (!c.isClosed() && !writer.isFinished()) {
            LockSupport.parkNanos(1000);
        }

        if (c.isClosed()) {
            LOGGER.info("finish to split dump file because the connection is closed.");
            dumpFileExecutor.stop();
            writer.stop();
            c.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, "finish to split dump file due to the connection is closed.");
            return;
        }

        printMsg(errors, c);
        LOGGER.info("finish to split dump file.");
    }

    private static void printMsg(List<ErrorMsg> errors, ManagerConnection c) {
        if (CollectionUtil.isEmpty(errors)) {
            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.setMessage(StringUtil.encode("please see detail in dump.log.", c.getCharset().getResults()));
            packet.write(c);
        } else {
            DumpFileError.execute(c, errors);
        }
    }

    private static DumpFileConfig parseOption(String options) {
        Matcher m = SPLIT_STMT.matcher(options);
        DumpFileConfig config = null;
        if (m.matches()) {
            config = new DumpFileConfig();
            config.setReadFile(m.group(1));
            config.setWritePath(m.group(2));
            if (m.group(4) != null) {
                config.setDefaultSchema(m.group(4));
            }
            if (m.group(6) != null) {
                config.setReadQueueSize(Integer.parseInt(m.group(6)));
            }
            if (m.group(8) != null) {
                config.setWriteQueueSize(Integer.parseInt(m.group(8)));
            }
            if (m.group(10) != null) {
                config.setMaxValues(Integer.parseInt(m.group(10)));
            }
        }
        return config;
    }

}
