package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.dump.*;
import com.actiontech.dble.manager.response.DumpFileError;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.CollectionUtil;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SplitDumpHandler {

    private static final Pattern SPLIT_STMT = Pattern.compile("([^\\s]+)\\s+([^\\s]+)\\s*(-r(\\d+))?\\s*(-w(\\d+))?\\s*(-l(\\d+))?", Pattern.CASE_INSENSITIVE);

    public void handle(String stmt, ManagerConnection c, int offset) {
        DumpFileConfig config = parseOption(stmt.substring(offset).trim());
        if (config == null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax");
            return;
        }

        DumpFileWriter writer = new DumpFileWriter();
        DumpFileReader reader = new DumpFileReader();
        DumpFileExecutor dumpFileExecutor;
        try {
            // firstly check file
            reader.open(config.getReadFile());
            writer.open(config.getWritePath(), config.getWriteQueueSize());

            // queue
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(config.getReadQueueSize());
            // thread for process table
            dumpFileExecutor = new DumpFileExecutor(queue, writer, config);
            new Thread(dumpFileExecutor).start();
            // start read
            writer.start();
            reader.start(queue);
        } catch (IOException e) {
            c.writeErrMessage(ErrorCode.ER_IO_EXCEPTION, e.getMessage());
            return;
        }

        while (!writer.isFinished()) {
            LockSupport.parkNanos(1000);
        }

        List<ErrorMsg> errors = dumpFileExecutor.getContext().getErrors();
        if (CollectionUtil.isEmpty(errors)) {
            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(c);
        } else {
            DumpFileError.execute(c, errors);
        }
    }

    private DumpFileConfig parseOption(String options) {
        Matcher m = SPLIT_STMT.matcher(options);
        DumpFileConfig config = null;
        if (m.matches()) {
            config = new DumpFileConfig();
            config.setReadFile(m.group(1));
            config.setWritePath(m.group(2));
            if (m.group(4) != null) {
                config.setReadQueueSize(Integer.parseInt(m.group(4)));
            }
            if (m.group(6) != null) {
                config.setWriteQueueSize(Integer.parseInt(m.group(6)));
            }
            if (m.group(8) != null) {
                config.setMaxValues(Integer.parseInt(m.group(8)));
            }
        }
        return config;
    }

}
