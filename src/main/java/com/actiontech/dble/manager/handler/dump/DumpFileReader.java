/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.handler.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
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
public final class DumpFileReader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpFileReader.class);
    private BlockingQueue<String> queue;
    private String fileName;

    public DumpFileReader(BlockingQueue<String> queue) {
        this.queue = queue;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void run() {
        FileChannel fileChannel = null;
        try {
            fileChannel = FileUtils.open(fileName, "r");
            ByteBuffer buffer = ByteBuffer.allocate(0x20000);
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                readSQLByEOF(buffer.array());
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
        } catch (IOException e) {
            LOGGER.error("Read dump file error: " + e.getMessage());
        } finally {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException e) {
                LOGGER.error("Close dump file error: " + e.getMessage());
            }
        }
    }

    // read one statement by ;
    private void readSQLByEOF(byte[] linesByte) {
        String[] lines = new String(linesByte, StandardCharsets.UTF_8).split(";");
        for (String line : lines) {
            this.queue.offer(line);
        }
    }

}
