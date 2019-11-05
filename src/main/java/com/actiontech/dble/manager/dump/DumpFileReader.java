package com.actiontech.dble.manager.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;

/**
 * @author Baofengqi
 */
public final class DumpFileReader {

    public static final Logger LOGGER = LoggerFactory.getLogger(DumpFileReader.class);
    public static final String EOF = "dump file eof";
    private String tempStr;
    private BlockingQueue<String> readQueue;
    private FileChannel fileChannel;

    public void open(String fileName) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
    }

    public void start(BlockingQueue<String> queue) throws IOException {
        try {
            this.readQueue = queue;
            ByteBuffer buffer = ByteBuffer.allocate(0x20000);
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                readSQLByEOF(buffer.array());
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            if (tempStr != null) {
                this.readQueue.put(tempStr);
                this.tempStr = null;
            }
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            // ignore
            LOGGER.warn(e.getMessage());
        } finally {
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
    private void readSQLByEOF(byte[] linesByte) throws InterruptedException {
        String[] lines = new String(linesByte, StandardCharsets.UTF_8).split(";");
        int length = lines.length;
        for (int i = 0; i < length; i++) {
            if (i == 0 && this.tempStr != null) {
                this.readQueue.put(tempStr + lines[0]);
                this.tempStr = null;
            } else if (i == length - 1 && !StringUtil.isEmpty(lines[i])) {
                this.tempStr = lines[i];
                continue;
            }
            this.readQueue.put(lines[i]);
        }
    }

}
