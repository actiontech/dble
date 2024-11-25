/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump;

import com.oceanbase.obsharding_d.backend.mysql.store.fs.FileUtils;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author Baofengqi
 */
public final class DumpFileReader implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";
    public static final Pattern CREATE_VIEW = Pattern.compile("CREATE\\s+VIEW\\s+`?([a-zA-Z_0-9\\-_]+)`?\\s+", Pattern.CASE_INSENSITIVE);
    public static final Pattern CREATE_VIEW1 = Pattern.compile("CREATE\\s+ALGORITHM", Pattern.CASE_INSENSITIVE);
    private FileChannel fileChannel;
    private long fileLength;
    private long readLength;
    private int readPercent;
    private final BlockingQueue<String> handleQueue;
    private DumpFileConfig fileConfig;
    private Map<String, String> errorMap;
    private AtomicBoolean errorFlag;

    public DumpFileReader(BlockingQueue<String> handleQueue, Map<String, String> map, AtomicBoolean flag) {
        this.handleQueue = handleQueue;
        this.errorMap = map;
        this.errorFlag = flag;
    }

    public void open(String fileName, DumpFileConfig config) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
        this.fileLength = this.fileChannel.size();
        this.fileConfig = config;
    }

    @Override
    public void run() {
        LOGGER.info("begin to read dump file.");
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("dump-file-read");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(fileConfig.getBufferSize());
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                readLength += byteRead;
                float percent = ((float) readLength / (float) fileLength) * 100;
                if (((int) percent) - readPercent > 5 || (int) percent == 100) {
                    readPercent = (int) percent;
                    LOGGER.info("dump file has bean read " + readPercent + "%");
                }
                String stmts = new String(buffer.array(), 0, byteRead, StandardCharsets.UTF_8);
                this.handleQueue.put(stmts);
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            this.handleQueue.put(EOF);
        } catch (IOException e) {
            LOGGER.warn("dump file exception", e);
            errorFlag.compareAndSet(false, true);
            errorMap.putIfAbsent("file reader exception", "reader exception,because:" + e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.debug("dump file reader is interrupted.");
        } catch (Error e) {
            LOGGER.warn("dump file error", e);
            errorFlag.compareAndSet(false, true);
            errorMap.putIfAbsent("file reader error", "reader error,because:" + e.getMessage());
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
}
