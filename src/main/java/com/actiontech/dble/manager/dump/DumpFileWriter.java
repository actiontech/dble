package com.actiontech.dble.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
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

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private static final String FILE_NAME_FORMAT = "%s-%s-%d.dump";
    private Map<String, DataNodeWriter> dataNodeWriters = new ConcurrentHashMap<>();
    private AtomicInteger finished = new AtomicInteger(0);

    public void open(String writePath, int writeQueueSize) throws IOException {
        Set<String> dataNodes = DbleServer.getInstance().getConfig().getDataNodes().keySet();
        Date date = new Date();
        for (String dataNode : dataNodes) {
            DataNodeWriter writer = new DataNodeWriter(dataNode, writeQueueSize);
            writer.open(String.format(FILE_NAME_FORMAT, writePath, dataNode, date.getTime()));
            dataNodeWriters.put(dataNode, writer);
        }
    }

    public void start() {
        for (Map.Entry<String, DataNodeWriter> entry : dataNodeWriters.entrySet()) {
            new Thread(entry.getValue(), entry.getKey() + "-writer-" + finished.incrementAndGet()).start();
        }
    }

    public void write(String dataNode, String stmt, boolean isChanged, boolean needEOF) throws InterruptedException {
        DataNodeWriter writer = this.dataNodeWriters.get(dataNode);
        if (writer != null) {
            if (isChanged) writer.write("\n");
            writer.write(stmt);
            if (needEOF) writer.write(";");
        }
    }

    public void write(String dataNode, String stmt) throws InterruptedException {
        write(dataNode, stmt, true, true);
    }

    public void writeAll(String stmt) throws InterruptedException {
        for (DataNodeWriter writer : dataNodeWriters.values()) {
            writer.write(stmt);
            writer.write(";");
        }
    }

    public boolean isFinished() {
        return finished.get() == 0;
    }

    class DataNodeWriter implements Runnable {
        private FileChannel fileChannel;
        private BlockingQueue<String> queue;
        private int queueSize;
        private String dataNode;

        DataNodeWriter(String dataNode, int queueSize) {
            this.dataNode = dataNode;
            this.queueSize = queueSize;
            this.queue = new ArrayBlockingQueue<>(queueSize);
        }

        void open(String fileName) throws IOException {
            if (FileUtils.exists(fileName)) {
                FileUtils.delete(fileName);
            }
            this.fileChannel = FileUtils.open(fileName, "rw");
        }

        void write(String stmt) throws InterruptedException {
            if (this.queue.size() == queueSize) {
                LOGGER.info("dump file write is too slow, please increase write queue size.");
            }
            this.queue.put(stmt);
        }

        void close() throws IOException {
            this.fileChannel.close();
            dataNodeWriters.remove(dataNode);
        }

        @Override
        public void run() {
            try {
                String stmt;
                long timestamp = System.currentTimeMillis();
                while (true) {
                    if (queue.isEmpty()) {
                        LOGGER.info("dump file executor is too slow, no good way.");
                    }
                    stmt = this.queue.take();
                    if (stmt.equals(DumpFileReader.EOF)) {
                        LOGGER.info("finish to write dump file.");
                        close();
                        finished.decrementAndGet();
                        return;
                    }
                    if (this.fileChannel != null) {
                        this.fileChannel.write(ByteBuffer.wrap(stmt.getBytes()));
                    }
                }
            } catch (IOException | InterruptedException e) {
                finished.decrementAndGet();
                LOGGER.warn("write " + dataNode + " dump file error, because:" + e.getMessage());
            } finally {
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
