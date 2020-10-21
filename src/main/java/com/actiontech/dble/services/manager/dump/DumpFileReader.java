package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

/**
 * @author Baofengqi
 */
public final class DumpFileReader {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";
    public static final Pattern CREATE_VIEW = Pattern.compile("CREATE\\s+VIEW\\s+`?([a-zA-Z_0-9\\-_]+)`?\\s+", Pattern.CASE_INSENSITIVE);
    private StringBuilder tempStr = new StringBuilder(200);
    private BlockingQueue<String> readQueue;
    private FileChannel fileChannel;
    private long fileLength;
    private long readLength;
    private int readPercent;

    public DumpFileReader(BlockingQueue<String> queue) {
        this.readQueue = queue;
    }

    public void open(String fileName) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
        this.fileLength = this.fileChannel.size();
    }

    public void start(ManagerService service, DumpFileExecutor executor) throws IOException, InterruptedException {
        LOGGER.info("begin to read dump file.");
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("dump-file-read");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(0x20000);
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                if (service.getConnection().isClosed()) {
                    LOGGER.info("finish to read dump file, because the connection is closed.");
                    executor.getContext().addError("finish to read dump file, because the connection is closed.");
                    executor.stop();
                    return;
                }
                if (executor.isStop()) {
                    LOGGER.info("finish to read dump file, because executor is stop.");
                    return;
                }
                readLength += byteRead;
                float percent = ((float) readLength / (float) fileLength) * 100;
                if (((int) percent) - readPercent > 5 || (int) percent == 100) {
                    readPercent = (int) percent;
                    LOGGER.info("dump file has bean read " + readPercent + "%");
                }
                readSQLByEOF(buffer.array(), byteRead);
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            if (tempStr != null) {
                this.readQueue.put(tempStr.toString());
                this.tempStr = null;
            }
            this.readQueue.put(EOF);
        } finally {
            TraceManager.finishSpan(traceObject);
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException e) {
                // ignore
                LOGGER.warn("close dump file error:" + e.getMessage());
            }
        }
    }

    // read one statement by ;
    private void readSQLByEOF(byte[] linesByte, int byteRead) throws InterruptedException {
        String stmts = new String(linesByte, 0, byteRead, StandardCharsets.UTF_8);
        boolean endWithEOF = stmts.endsWith(";");
        String[] lines = stmts.split(";\\r?\\n");
        int len = lines.length - 1;

        int i = 0;
        if (len > 0 && tempStr != null) {
            tempStr.append(lines[0]);
            this.readQueue.put(tempStr.toString());
            tempStr = null;
            i = 1;
        }

        for (; i < len; i++) {
            this.readQueue.put(lines[i]);
        }

        if (!endWithEOF) {
            if (tempStr == null) {
                tempStr = new StringBuilder(lines[len]);
            } else {
                tempStr.append(lines[len]);
            }
        } else {
            if (tempStr != null) {
                tempStr.append(lines[len]);
                this.readQueue.put(tempStr.toString());
                tempStr = null;
            } else {
                this.readQueue.put(lines[len]);
            }
        }
    }

}
