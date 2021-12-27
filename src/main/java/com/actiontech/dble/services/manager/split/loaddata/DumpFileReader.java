package com.actiontech.dble.services.manager.split.loaddata;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.services.manager.dump.ErrorMsg;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DumpFileReader implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";
    private FileChannel fileChannel;
    private long fileLength;
    private long readLength;
    private int readPercent;
    private final BlockingQueue<String> handleQueue;

    private final List<ErrorMsg> errorMsgList;
    private final AtomicBoolean errorFlag;
    private int bufferSize;

    public DumpFileReader(BlockingQueue<String> handleQueue, List<ErrorMsg> errorMsgList, AtomicBoolean errorFlag) {
        this.handleQueue = handleQueue;
        this.errorMsgList = errorMsgList;
        this.errorFlag = errorFlag;
    }

    public void open(String fileName, int fileBufferSize) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
        this.fileLength = this.fileChannel.size();
        this.bufferSize = fileBufferSize;
    }

    @Override
    public void run() {
        LOGGER.info("begin to read dump file.");
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("dump-file-read");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
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
        } catch (InterruptedException e) {
            LOGGER.debug("dump file reader is interrupted.");
        } catch (Exception | Error e) {
            LOGGER.warn("split loaddata error,", e);
            errorFlag.compareAndSet(false, true);
            errorMsgList.add(new ErrorMsg("split loaddata unknown error[exit]", e.getMessage()));
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

