package com.actiontech.dble.manager.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
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
    public static final Pattern HINT = Pattern.compile("/\\*!\\d+\\s+(.*)\\*/", Pattern.CASE_INSENSITIVE);
    private StringBuilder tempStr = new StringBuilder(200);
    private BlockingQueue<String> readQueue;
    private FileChannel fileChannel;

    public DumpFileReader(BlockingQueue<String> queue) {
        this.readQueue = queue;
    }

    public void open(String fileName) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
    }

    public void start() throws IOException {
        LOGGER.info("begin to read dump file.");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(0x20000);
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                readSQLByEOF(buffer.array(), byteRead);
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            if (tempStr != null) {
                this.readQueue.put(tempStr.toString());
                this.tempStr = null;
            }
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            // ignore
            LOGGER.warn("thread for read dump file is interrupted.");
        } finally {
            LOGGER.info("finish to read dump file.");
            try {
                this.readQueue.put(EOF);
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException | InterruptedException e) {
                // ignore
                LOGGER.warn("close dump file error:" + e.getMessage());
            }
        }
    }

    // read one statement by ;
    private void readSQLByEOF(byte[] linesByte, int byteRead) throws InterruptedException {
        String stmts = new String(linesByte, 0, byteRead, StandardCharsets.UTF_8);
        boolean endWithEOF = stmts.endsWith(";");
        String[] lines = stmts.split(";");
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
