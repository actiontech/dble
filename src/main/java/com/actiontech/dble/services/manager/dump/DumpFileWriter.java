package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DumpFileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private static final String FILE_NAME_FORMAT = "%s-%s-%d.dump";
    private Map<String, ShardingNodeWriter> shardingNodeWriters = new ConcurrentHashMap<>();
    private AtomicInteger finished = new AtomicInteger(0);
    private volatile boolean isDeleteFile = false;

    public void open(String writePath, int writeQueueSize) throws IOException {
        Set<String> shardingNodes = DbleServer.getInstance().getConfig().getShardingNodes().keySet();
        Date date = new Date();
        for (String shardingNode : shardingNodes) {
            ShardingNodeWriter writer = new ShardingNodeWriter(shardingNode, writeQueueSize);
            writer.open(String.format(FILE_NAME_FORMAT, writePath, shardingNode, date.getTime()));
            shardingNodeWriters.put(shardingNode, writer);
        }
    }

    public void start() {
        Thread writer;
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            writer = new Thread(entry.getValue(), entry.getKey() + "-writer-" + finished.incrementAndGet());
            writer.start();
            entry.getValue().self = writer;
        }
    }

    public void stop() {
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            if (entry.getValue().self != null) {
                entry.getValue().self.interrupt();
            }
        }
    }

    public void write(String shardingNode, String stmt, boolean isChanged, boolean needEOF) throws InterruptedException {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            if (writer.isAddEof()) {
                writer.write(";");
                writer.setAddEof(false);
            }
            if (isChanged) writer.write("\n");
            writer.write(stmt);
            if (needEOF) writer.write(";");
        }
    }

    public void write(String shardingNode, String stmt) throws InterruptedException {
        write(shardingNode, stmt, false, true);
    }

    public void writeInsertHeader(String shardingNode, String stmt) throws InterruptedException {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write("\n");
            writer.write(stmt);
            writer.setAddEof(true);
        }
    }

    public void writeInsertValues(String shardingNode, String stmt) throws InterruptedException {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(stmt);
        }
    }

    public void writeAll(String stmt) throws InterruptedException {
        for (ShardingNodeWriter writer : shardingNodeWriters.values()) {
            writer.write(stmt);
            writer.write(";");
        }
    }

    public boolean isFinished() {
        return finished.get() == 0;
    }

    public void setDeleteFile(boolean deleteFile) {
        this.isDeleteFile = deleteFile;
    }

    class ShardingNodeWriter implements Runnable {
        private FileChannel fileChannel;
        private BlockingQueue<String> queue;
        private int queueSize;
        private String shardingNode;
        private String path;
        private Thread self;
        // insert values eof
        private boolean addEof = false;

        ShardingNodeWriter(String shardingNode, int queueSize) {
            this.shardingNode = shardingNode;
            this.queueSize = queueSize;
            this.queue = new ArrayBlockingQueue<>(queueSize);
        }

        public void setAddEof(boolean addEof) {
            this.addEof = addEof;
        }

        public boolean isAddEof() {
            return addEof;
        }

        void open(String fileName) throws IOException {
            this.path = fileName;
            this.fileChannel = FileUtils.open(fileName, "rw");
        }

        void write(String stmt) throws InterruptedException {
            this.queue.put(stmt);
        }

        void close() throws IOException {
            this.fileChannel.close();
            shardingNodeWriters.remove(shardingNode);
            if (isDeleteFile) {
                FileUtils.delete(path);
            }
        }

        @Override
        public void run() {
            try {
                String stmt;
                long startTime = TimeUtil.currentTimeMillis();
                while (!Thread.currentThread().isInterrupted()) {
                    stmt = this.queue.take();
                    if (LOGGER.isDebugEnabled()) {
                        long endTime = TimeUtil.currentTimeMillis();
                        if (endTime - startTime > 1000) {
                            startTime = endTime;
                            if (queue.isEmpty()) {
                                LOGGER.debug("dump file executor parse statement slowly.");
                            } else if (this.queue.size() == queueSize) {
                                LOGGER.debug("dump file writer is slow, you can try increasing write queue size.");
                            }
                        }
                    }

                    if (stmt.equals(DumpFileReader.EOF)) {
                        LOGGER.info("finish to write dump file.");
                        close();
                        return;
                    }
                    if (this.fileChannel != null) {
                        this.fileChannel.write(ByteBuffer.wrap(stmt.getBytes()));
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("dump file writer[" + shardingNode + "] occur error:" + e.getMessage());
            } catch (InterruptedException ie) {
                LOGGER.warn("dump file writer[" + shardingNode + "] is interrupted.");
            } finally {
                finished.decrementAndGet();
                try {
                    close();
                } catch (IOException e) {
                    // ignore
                    LOGGER.warn("close dump file error, because:" + e.getMessage());
                }
            }
        }
    }

}
