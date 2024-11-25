/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.store.fs.FileUtils;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.dump.handler.ShardingValuesHandler;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.base.Strings;
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
    private final EventTranslatorOneArg<Element, Object> translator = (event, sequence, arg0) -> event.set(arg0);
    private Map<String, String> writerErrorMap;

    public DumpFileWriter(Map<String, String> writerErrorMap) {
        this.writerErrorMap = writerErrorMap;
    }

    public void open(String writePath, int writeQueueSize, int maxValue) throws IOException {
        Set<String> shardingNodes = OBsharding_DServer.getInstance().getConfig().getShardingNodes().keySet();
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

    public void stop(boolean errorFlag) throws IOException {
        for (Map.Entry<String, ShardingNodeWriter> entry : shardingNodeWriters.entrySet()) {
            entry.getValue().close(errorFlag);
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

    public void writeInsertHeader(String shardingNode, ShardingValuesHandler.InsertQuery insertQuery) {
        ShardingNodeWriter writer = this.shardingNodeWriters.get(shardingNode);
        if (writer != null) {
            writer.write(insertQuery);
        }
    }

    public void writeAll(String obj) {
        for (ShardingNodeWriter writer : shardingNodeWriters.values()) {
            writer.write(obj);
        }
    }

    public boolean isFinished() {
        return finished.get() <= 0;
    }

    public void setDeleteFile(boolean deleteFile) {
        this.isDeleteFile = deleteFile;
    }


    static class Element {

        private Object value;

        Element() {
        }

        public Object get() {
            return value;
        }

        public void set(Object val) {
            this.value = val;
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
        private boolean error = false;

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
            if (null == this.disruptor || error) {
                return;
            }
            if (!disruptor.getRingBuffer().tryPublishEvent(translator, obj)) {
                disruptor.getRingBuffer().publishEvent(translator, obj);
            }
        }

        void close(boolean errorFlag) throws IOException {
            this.bufferedWriter.close();
            if (isDeleteFile || errorFlag) {
                FileUtils.delete(path);
            }
        }

        public void start() {
            String wrapStr = ";\n";
            EventFactory<Element> factory = Element::new;
            EventHandler<Element> handler = (element, sequence, endOfBatch) -> {
                if (error) {
                    return;
                }
                try {
                    Object content = element.get();

                    if (null != content && content.equals(DumpFileReader.EOF)) {
                        this.bufferedWriter.write(wrapStr);
                        LOGGER.info("finish to write dump file.");
                        close(false);
                        finished.decrementAndGet();
                        return;
                    }
                    writeContent(content, wrapStr);
                } catch (Exception | Error e) {
                    error = true;
                    finished.decrementAndGet();
                    writerErrorMap.putIfAbsent(this.shardingNode, "writer error,because:" + e.getMessage());
                    close(true);
                }
            };
            SleepingWaitStrategy strategy = new SleepingWaitStrategy();
            try {
                disruptor = new Disruptor(factory, this.queueSize, new ThreadFactoryBuilder().setNameFormat("splitWriter" + upperCaseFirst(shardingNode)).build(), ProducerType.MULTI, strategy);
            } catch (IllegalArgumentException e) {
                throw new DumpException("The value of -w needs to be a power of 2");
            }
            disruptor.handleEventsWith(handler);
            disruptor.setDefaultExceptionHandler(new SplitWriterExceptionHandler());
            disruptor.start();
        }


        private void writeContent(Object obj, String wrapStr) throws IOException {
            String table = null;
            String content = null;
            ShardingValuesHandler.InsertQuery insertQuery = null;
            if (obj instanceof ShardingValuesHandler.InsertQuery) {
                insertQuery = (ShardingValuesHandler.InsertQuery) obj;
                table = StringUtil.removeBackQuote(insertQuery.getInsertQueryPos().getTableName());
            } else {
                content = wrapStr + obj;
            }

            if (table != null && (table.equals(this.currentTable) || table.equalsIgnoreCase(this.currentTable)) && this.rows < maxValues) {
                //splicing insert
                writeInsertValue(insertQuery);
                rows++;
            } else if (table != null) {
                this.currentTable = table;
                writeInsert(table, insertQuery, wrapStr);
                rows = 1;
            } else {
                if (this.bufferedWriter != null) {
                    this.bufferedWriter.write(content);
                }
            }
        }

        private void writeInsert(String table, ShardingValuesHandler.InsertQuery insertQuery, String wrapStr) throws IOException {
            // add
            StringBuilder insertHeader = new StringBuilder(200);
            insertHeader.append("INSERT ");
            if (insertQuery.getInsertQueryPos().isIgnore()) {
                insertHeader.append("IGNORE ");
            }
            insertHeader.append("INTO ");
            insertHeader.append('`');
            insertHeader.append(table);
            insertHeader.append('`');
            if (!CollectionUtil.isEmpty(insertQuery.getInsertQueryPos().getColumns())) {
                Integer start = insertQuery.getInsertQueryPos().getColumnRange().getKey();
                Integer end = insertQuery.getInsertQueryPos().getColumnRange().getValue();
                insertHeader.append(' ');
                insertHeader.append('(');
                insertHeader.append(insertQuery.getInsertQueryPos().getInsertChars(), start, end - start);
                insertHeader.append(')');
                insertHeader.append(' ');
            }
            insertHeader.append(" VALUES ");
            insertHeader.append('(');
            if (insertQuery.getIncrementColumnIndex() != -1) {
                //has increment column
                int index = 0;
                for (Pair<Integer, Integer> pair : insertQuery.getValuePair()) {
                    Integer start = pair.getKey();
                    Integer end = pair.getValue();
                    if (index++ == insertQuery.getIncrementColumnIndex()) {
                        insertHeader.append(insertQuery.getIncrementColumnValue());
                    } else {
                        insertHeader.append(insertQuery.getInsertQueryPos().getInsertChars(), start, end - start);
                    }
                    if (index != insertQuery.getValuePair().size()) {
                        insertHeader.append(',');
                    }
                }
            } else {
                Integer start = insertQuery.getValuePair().get(0).getKey();
                Integer end = insertQuery.getValuePair().get(insertQuery.getValuePair().size() - 1).getValue();
                insertHeader.append(insertQuery.getInsertQueryPos().getInsertChars(), start, end - start);
            }
            insertHeader.append(')');
            String content = wrapStr + insertHeader.toString();
            if (this.bufferedWriter != null) {
                this.bufferedWriter.write(content);
            }
        }

        private void writeInsertValue(ShardingValuesHandler.InsertQuery insertQuery) throws IOException {
            char[] insertChars = insertQuery.getInsertQueryPos().getInsertChars();
            if (this.bufferedWriter != null) {
                this.bufferedWriter.write(',');
                this.bufferedWriter.write('(');
                if (insertQuery.getIncrementColumnIndex() != -1) {
                    //has increment column
                    int index = 0;
                    for (Pair<Integer, Integer> pair : insertQuery.getValuePair()) {
                        Integer start = pair.getKey();
                        Integer end = pair.getValue();
                        if (index++ == insertQuery.getIncrementColumnIndex()) {
                            this.bufferedWriter.write(insertQuery.getIncrementColumnValue() + "");
                        } else {
                            this.bufferedWriter.write(insertQuery.getInsertQueryPos().getInsertChars(), start, end - start);
                        }
                        if (index != insertQuery.getValuePair().size()) {
                            this.bufferedWriter.write(',');
                        }
                    }
                } else {
                    Integer start = insertQuery.getValuePair().get(0).getKey();
                    Integer end = insertQuery.getValuePair().get(insertQuery.getValuePair().size() - 1).getValue();
                    this.bufferedWriter.write(insertChars, start, end - start);
                }
                this.bufferedWriter.write(')');
            }
        }

        private String upperCaseFirst(String val) {
            if (Strings.isNullOrEmpty(val)) {
                return val;
            }
            char[] arr = val.toCharArray();
            arr[0] = Character.toUpperCase(arr[0]);
            return new String(arr);
        }
    }

    // exception
    public static final class SplitWriterExceptionHandler implements ExceptionHandler {

        public SplitWriterExceptionHandler() {
        }

        @Override
        public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.warn("Exception processing: {} {} ,exception：{}", sequence, event, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOGGER.error("Exception during onStart for split disruptor ,exception：{}", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("Exception during onShutdown for split disruptor ,exception：{}", ex);
        }
    }


}
