package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.dump.*;
import com.actiontech.dble.services.manager.response.DumpFileError;
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

    private static final Pattern SPLIT_STMT = Pattern.compile("([^\\s]+)\\s+([^\\s]+)(((\\s*(-s([^\\s]+))?)|(\\s+(-r(\\d+))?)|(\\s+(-w(\\d+))?)|(\\s+(-l(\\d+))?)|(\\s+(--ignore)?))+)", Pattern.CASE_INSENSITIVE);
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
            defaultSchemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(config.getDefaultSchema());
            if (defaultSchemaConfig == null) {
                String errMsg = "Default schema[" + config.getDefaultSchema() + "] doesn't exist in config.";
                LOGGER.info(errMsg);
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, errMsg);
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
            reader.start(service, dumpFileExecutor);
        } catch (IOException | InterruptedException e) {
            LOGGER.info("finish to split dump file because " + e.getMessage());
            service.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, e.getMessage());
            return;
        }

        List<ErrorMsg> errors = dumpFileExecutor.getContext().getErrors();
        if (!CollectionUtil.isEmpty(errors)) {
            DumpFileError.execute(service, errors);
            return;
        }

        while (!service.getConnection().isClosed() && !writer.isFinished()) {
            LockSupport.parkNanos(1000);
        }

        if (service.getConnection().isClosed()) {
            LOGGER.info("finish to split dump file because the connection is closed.");
            dumpFileExecutor.stop();
            writer.stop();
            service.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, "finish to split dump file due to the connection is closed.");
            return;
        }

        printMsg(errors, service);
        LOGGER.info("finish to split dump file.");
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
        }
        return config;
    }

}
