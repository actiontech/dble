package com.actiontech.dble.services.manager.split.loaddata;


import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.btrace.provider.SplitFileProvider;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.services.manager.dump.ErrorMsg;
import com.actiontech.dble.services.manager.handler.SplitLoadDataHandler;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.NameableThreadFactory;
import com.actiontech.dble.util.StringUtil;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


public final class DumpFileHandler implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");

    private final BlockingQueue<String> handleQueue;

    private StringBuilder tempStr = new StringBuilder(1000);

    private final AtomicInteger nodeCount;

    private final List<ErrorMsg> errorMsgList;
    private final AtomicBoolean errorFlag;
    private final Map<String, ShardingNodeWriter> writerMap;
    private final SplitLoadDataHandler.Config config;
    private AbstractPartitionAlgorithm algorithm;
    private int shardingColumnIndex;
    private List<String> shardingNodes;
    private Disruptor<ShardingNodeWriter.Element> disruptor;
    private final AtomicLong lineCount = new AtomicLong(-1L);

    private final EventTranslatorOneArg<ShardingNodeWriter.Element, String> translator = (event, sequence, arg0) -> event.set(arg0);


    public DumpFileHandler(BlockingQueue<String> handleQueue, SplitLoadDataHandler.Config config, AtomicInteger nodeCount, Map<String, ShardingNodeWriter> writerMap,
                           List<ErrorMsg> errorMsgList, AtomicBoolean errorFlag) {
        this.handleQueue = handleQueue;
        this.config = config;
        this.nodeCount = nodeCount;
        this.writerMap = writerMap;
        this.errorMsgList = errorMsgList;
        this.errorFlag = errorFlag;
    }

    public void open() {
        this.algorithm = ((ShardingTableConfig) config.getTableConfig()).getFunction();
        this.shardingNodes = config.getTableConfig().getShardingNodes();

        //shardingColumn index
        TableMeta tableMeta = ProxyMeta.getInstance().getTmManager().getCatalogs().get(config.getSchemaName()).getTableMeta(config.getTableName());
        for (ColumnMeta column : tableMeta.getColumns()) {
            if (column.getName().equalsIgnoreCase(((ShardingTableConfig) config.getTableConfig()).getShardingColumn())) {
                break;
            }
            this.shardingColumnIndex++;
        }


        //executor-disruptor
        EventFactory<ShardingNodeWriter.Element> factory = ShardingNodeWriter.Element::new;
        SplitEventConsumer[] consumers = new SplitEventConsumer[config.getExecutorCount()];
        for (int i = 0; i < consumers.length; i++) {
            consumers[i] = new SplitEventConsumer();
        }
        this.disruptor = new Disruptor<>(factory, config.getDisruptorBufferSize(), new NameableThreadFactory("Split_Executor", true),
                ProducerType.SINGLE, new BlockingWaitStrategy());
        this.disruptor.handleEventsWithWorkerPool(consumers);
        this.disruptor.setDefaultExceptionHandler(new SplitWriterExceptionHandler());
        this.disruptor.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                String stmts = handleQueue.take();
                SplitFileProvider.getHandleQueueSizeOfTake(handleQueue.size());
                if (stmts.isEmpty()) {
                    continue;
                }
                if (stmts.equals(DumpFileReader.EOF)) {
                    if (null != tempStr && !StringUtil.isBlank(tempStr.toString())) {
                        putLine(tempStr.toString());
                        this.tempStr = null;
                    }
                    putLine(DumpFileReader.EOF);
                    break;
                }
                readLine(stmts);
            } catch (InterruptedException e) {
                LOGGER.debug("dump file handler is interrupted.");
                break;
            } catch (Exception | Error e) {
                //other-exception & oom error... should exit
                LOGGER.warn("split loaddata error,", e);
                errorFlag.compareAndSet(false, true);
                errorMsgList.add(new ErrorMsg("split loaddata unknown error[exit]", e.getMessage()));
                break;
            }
        }

    }

    // read one statement by ;\n
    private void readLine(String stmts) {
        boolean endWithEOF = stmts.endsWith("\n") | stmts.endsWith("\r\n");
        String[] split = stmts.split("\n");
        int len = split.length - 1;

        int i = 0;
        if (len > 0 && tempStr != null && !StringUtil.isEmpty(tempStr.toString())) {
            tempStr.append(split[0]);
            putLine(tempStr.toString());
            tempStr = null;
            i = 1;
        }

        for (; i < len; i++) {
            if (!StringUtil.isEmpty(split[i])) {
                putLine(split[i]);
            }
        }


        if (!endWithEOF) {
            if (tempStr == null) {
                tempStr = new StringBuilder(split[len]);
            } else {
                tempStr.append(split[len]);
            }
        } else {
            if (tempStr != null && !StringUtil.isEmpty(tempStr.toString())) {
                tempStr.append(split[len]);
                putLine(tempStr.toString());
                tempStr = null;
            } else {
                if (!StringUtil.isEmpty(split[len])) {
                    putLine(split[len]);
                }
            }
        }
    }

    public void putLine(String line) {
        if (line == null) {
            return;
        }
        if (!this.lineCount.compareAndSet(-1, 1)) {
            this.lineCount.incrementAndGet();
        }
        this.disruptor.getRingBuffer().publishEvent(translator, line);
    }

    public static String getDataColumn(String content, int index) {
        int start = content.indexOf(",");
        index--;
        if (index >= 0) {
            while (index != 0) {
                index--;
                start = content.indexOf(",", start + 1);
            }
            if (start < 0) {
                throw new RuntimeException();
            }
        } else {
            start = index;
        }
        int end = content.indexOf(",", start + 1);
        if (end < 0) {
            end = content.length();
        }
        return content.substring(start + 1, end);
    }

    public void shutDownExecutor() {
        while (lineCount.get() != 0) {
            LockSupport.parkNanos(1000);
        }
        this.disruptor.shutdown();
    }

    class SplitEventConsumer implements WorkHandler<ShardingNodeWriter.Element> {

        @Override
        public void onEvent(ShardingNodeWriter.Element element) {
            try {
                String line = element.get();
                if (errorFlag.get()) {
                    return;
                }

                if (line.equals(DumpFileReader.EOF)) {
                    while (lineCount.get() != 1) {
                        LockSupport.parkNanos(1000);
                    }
                    for (Map.Entry<String, ShardingNodeWriter> nodeWriterEntry : writerMap.entrySet()) {
                        nodeWriterEntry.getValue().write(DumpFileReader.EOF, errorFlag);
                    }
                    return;
                }
                String shardingColumnVal = getDataColumn(line, shardingColumnIndex);
                Integer nodeIndex = algorithm.calculate(shardingColumnVal);
                String shardingNode = shardingNodes.get(nodeIndex);

                ShardingNodeWriter initialization = new ShardingNodeWriter();
                ShardingNodeWriter shardingNodeWriter = writerMap.computeIfAbsent(shardingNode, key -> initialization);
                if (initialization == shardingNodeWriter) {
                    //init writer
                    String fileName = String.format("%s-%s.csv", config.getTargetPath() + FileUtils.getName(config.getSourcePath()), shardingNode);
                    shardingNodeWriter.open(fileName, shardingNode, nodeCount, errorMsgList, errorFlag, config);
                }
                shardingNodeWriter.write(line, errorFlag);
            } catch (Exception | Error e) {
                LOGGER.warn("split writer error,", e);
                errorFlag.compareAndSet(false, true);
                errorMsgList.add(new ErrorMsg("split executor error[exit]", e.getMessage()));
            } finally {
                lineCount.decrementAndGet();
            }
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

