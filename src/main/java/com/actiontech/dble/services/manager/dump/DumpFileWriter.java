package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.manager.dump.handler.InsertHandler;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLSyntaxErrorException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class DumpFileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private static final String FILE_NAME_FORMAT = "%s-%s-%d.dump";
    private final Map<String, ShardingNodeWriter> shardingNodeWriters = new ConcurrentHashMap<>();
    private final AtomicInteger finished = new AtomicInteger(0);
    private volatile boolean isDeleteFile = false;
    private int maxValues;

    public void open(String writePath, int writeQueueSize, int maxValue) throws IOException {
        Set<String> shardingNodes = DbleServer.getInstance().getConfig().getShardingNodes().keySet();
        Date date = new Date();
        for (String shardingNode : shardingNodes) {
            ShardingNodeWriter writer = new ShardingNodeWriter(shardingNode, writeQueueSize);
            writer.open(String.format(FILE_NAME_FORMAT, writePath, shardingNode, date.getTime()));
            shardingNodeWriters.put(shardingNode, writer);
        }
        this.maxValues = maxValue;
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

    public void write(String shardingNode, String stmt) throws InterruptedException {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(stmt);
        }
    }

    public void writeInsertHeader(String shardingNode, String stmt) {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(stmt);
        }
    }

    public void writeAll(String stmt) throws InterruptedException {
        for (ShardingNodeWriter writer : shardingNodeWriters.values()) {
            writer.write(stmt);
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
        private final BlockingQueue<String> queue;
        private final int queueSize;
        private final String shardingNode;
        private String path;
        private Thread self;
        private String currentTable;
        private int rows = 1;
        private boolean isFirst = true;

        ShardingNodeWriter(String shardingNode, int queueSize) {
            this.shardingNode = shardingNode;
            this.queueSize = queueSize;
            this.queue = new ArrayBlockingQueue<>(queueSize);
        }

        public BlockingQueue<String> getQueue() {
            return queue;
        }

        void open(String fileName) throws IOException {
            this.path = fileName;
            this.fileChannel = FileUtils.open(fileName, "rw");
        }

        void write(String stmt) {
            try {
                this.queue.put(stmt);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        void close() throws IOException {
            this.fileChannel.close();
            shardingNodeWriters.remove(shardingNode);
            if (isDeleteFile) {
                FileUtils.delete(path);
            }
        }

        public void setCurrentTable(String currentTable) {
            this.currentTable = currentTable;
        }

        @Override
        public void run() {
            String wrapStr = ";\n";
            try {
                String stmt;
                long startTime = TimeUtil.currentTimeMillis();
                while (!Thread.currentThread().isInterrupted()) {
                    stmt = this.queue.take();
                    if (StringUtil.isBlank(stmt)) {
                        continue;
                    }
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
                        this.fileChannel.write(ByteBuffer.wrap(wrapStr.getBytes()));
                        LOGGER.info("finish to write dump file.");
                        close();
                        return;
                    }
                    writeContent(stmt, wrapStr);
                }
            } catch (IOException e) {
                LOGGER.warn("dump file writer[" + shardingNode + "] occur error:" + e.getMessage());
            } catch (SQLSyntaxErrorException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
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

        private void writeContent(String stmt, String wrapStr) throws IOException, SQLSyntaxErrorException {
            String table = null;
            Matcher matcher = InsertHandler.INSERT_STMT.matcher(stmt);
            if (matcher.find()) {
                table = matcher.group(2);
            }

            if (table != null && table.equalsIgnoreCase(this.currentTable) && this.rows < maxValues) {
                //splicing insert
                MySqlInsertStatement insert = (MySqlInsertStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
                if (insert.getValuesList().size() == 1) {
                    stmt = getSqlStr(insert.getValuesList().get(0).getValues());
                }
                rows++;
            } else if (!isFirst) {
                stmt = wrapStr + stmt;
                rows = 1;
            }
            this.currentTable = table;
            if (this.fileChannel != null) {
                isFirst = false;
                this.fileChannel.write(ByteBuffer.wrap(stmt.getBytes()));
            }
        }

        protected String getSqlStr(List<SQLExpr> values) {
            StringBuilder sbValues = new StringBuilder();
            sbValues.append(",(");
            for (int i = 0; i < values.size(); i++) {
                if (i != 0) {
                    sbValues.append(",");
                }
                sbValues.append(values.get(i).toString());
            }
            sbValues.append(")");
            return sbValues.toString();
        }
    }

}
