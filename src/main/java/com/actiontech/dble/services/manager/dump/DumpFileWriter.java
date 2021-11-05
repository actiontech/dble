package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.log.general.GeneralLogDisruptor;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DumpFileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private static final String FILE_NAME_FORMAT = "%s-%s-%d.dump";
    private final Map<String, ShardingNodeWriter> shardingNodeWriters = new ConcurrentHashMap<>();
    private final AtomicInteger finished = new AtomicInteger(0);
    private volatile boolean isDeleteFile = false;
    private int maxValues;
    // translator
    private EventTranslatorOneArg<Element, Object> TRANSLATOR = (event, sequence, arg0) -> event.set(arg0);

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
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            finished.incrementAndGet();
            entry.getValue().start();
        }
    }

    public void stop() {
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            entry.getValue().disruptor.shutdown();
        }
    }

    public void shutdown() {
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            entry.getValue().disruptor.shutdown();
        }
        shardingNodeWriters.clear();
    }

    public void write(String shardingNode, String stmt) {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(stmt);
        }
    }

    public void write(String shardingNode, SQLStatement statement) {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(statement);
        }
    }

    public void writeInsertHeader(String shardingNode, SQLStatement statement) {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(statement);
        }
    }

    public void writeAll(Object obj) {
        for (ShardingNodeWriter writer : shardingNodeWriters.values()) {
            writer.write(obj);
        }
    }

    public boolean isFinished() {
        return finished.get() == 0;
    }

    public void setDeleteFile(boolean deleteFile) {
        this.isDeleteFile = deleteFile;
    }

    class Element {

        private Object value;

        public Element() {
        }

        public Element(Object value) {
            this.value = value;
        }

        public Object get() {
            return value;
        }

        public void set(Object value) {
            this.value = value;
        }

    }

    class ShardingNodeWriter {
        private BufferedWriter bufferedWriter;
        private final BlockingQueue<Object> queue;
        private Disruptor<Element> disruptor;
        private final int queueSize;
        private final String shardingNode;
        private String path;
        private String currentTable;
        private int rows = 1;
        private boolean isFirst = true;

        ShardingNodeWriter(String shardingNode, int queueSize) {
            this.shardingNode = shardingNode;
            this.queueSize = queueSize;
            this.queue = new ArrayBlockingQueue<>(queueSize);
        }

        public BlockingQueue<Object> getQueue() {
            return queue;
        }

        void open(String fileName) throws IOException {
            this.path = fileName;
            this.bufferedWriter = new BufferedWriter(new FileWriter(fileName));
        }

        void write(Object obj) {
            if (null == this.disruptor) {
                return;
            }
            if (!disruptor.getRingBuffer().tryPublishEvent(TRANSLATOR, obj)) {
                disruptor.getRingBuffer().publishEvent(TRANSLATOR, obj);
            }
        }

        void close() throws IOException {
            this.bufferedWriter.close();
            if (isDeleteFile) {
                FileUtils.delete(path);
            }
        }

        public void start() {
            String wrapStr = ";\n";
            EventFactory<Element> factory = () -> new Element();
            EventHandler<Element> handler = (element, sequence, endOfBatch) -> {
                SQLInsertStatement insertStatement = null;
                Object obj = element.get();
                String content = null;
                if (obj instanceof SQLInsertStatement) {
                    insertStatement = (SQLInsertStatement) obj;
                } else {
                    content = (String) obj;
                }
                if (null != content && content.equals(DumpFileReader.EOF)) {
                    this.bufferedWriter.write(wrapStr);
                    LOGGER.info("finish to write dump file.");
                    close();
                    finished.decrementAndGet();
                    return;
                }
                writeContent(insertStatement, wrapStr, content);
            };
            SleepingWaitStrategy strategy = new SleepingWaitStrategy();
            this.disruptor = new Disruptor(factory, this.queueSize, new ThreadFactoryBuilder().setNameFormat("dis-split-" + shardingNode).build(), ProducerType.MULTI, strategy);
            this.disruptor.handleEventsWith(handler);
            this.disruptor.setDefaultExceptionHandler(new GeneralLogDisruptor.GeneralLogExceptionHandler());
            this.disruptor.start();
        }

        private void writeContent(SQLInsertStatement insertStatement, String wrapStr, String content) throws IOException {
            String table = null;
            if (insertStatement != null) {
                table = StringUtil.removeBackQuote(insertStatement.getTableName().getSimpleName());
            }

            if (table != null && (table.equals(this.currentTable) || table.equalsIgnoreCase(this.currentTable)) && this.rows < maxValues) {
                //splicing insert
                if (insertStatement.getValuesList().size() == 1) {
                    content = getSqlStr(insertStatement.getValuesList().get(0).getValues());
                }
                rows++;
            } else if (!isFirst) {
                if (null != insertStatement) {
                    content = wrapStr + insertStatement.toString();
                } else {
                    content = wrapStr + content;
                }
                rows = 1;
            }
            this.currentTable = table;
            if (this.bufferedWriter != null) {
                isFirst = false;
                this.bufferedWriter.write(content);
            }
        }

        protected String getSqlStr(List<SQLExpr> values) {
            StringBuilder sbValues = new StringBuilder(200);
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
