/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.split.loaddata;


import com.oceanbase.obsharding_d.services.manager.dump.DumpException;
import com.oceanbase.obsharding_d.services.manager.dump.ErrorMsg;
import com.oceanbase.obsharding_d.services.manager.handler.SplitLoadDataHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class ShardingNodeWriter {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private BufferedWriter bufferedWriter;

    // translator
    private final EventTranslatorOneArg<Element, String[]> translator = (Element event, long sequence, String[] arg0) -> event.setValue(arg0);

    private Disruptor<Element> disruptor;

    private AtomicInteger nodeCount;

    private List<ErrorMsg> errorMsgList;
    private AtomicBoolean errorFlag;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ShardingNodeWriter() {
    }

    public void open(String fileName, String shardingNode, AtomicInteger nodeCountVal, List<ErrorMsg> errorMsgListVal, AtomicBoolean errorFlagVal,
                     SplitLoadDataHandler.Config config) throws IOException {
        FileWriter fileWriter = new FileWriter(new File(fileName), false);
        bufferedWriter = new BufferedWriter(fileWriter);
        this.nodeCount = nodeCountVal;
        this.errorFlag = errorFlagVal;
        this.errorMsgList = errorMsgListVal;
        if (!this.nodeCount.compareAndSet(-1, 1)) {
            this.nodeCount.incrementAndGet();
        }

        EventFactory<Element> factory = Element::new;
        EventHandler<Element> handler = (element, sequence, endOfBatch) -> {
            try {
                String[] content = element.getValue();
                if (null == content) {
                    return;
                }
                if (content[0].equals(DumpFileReader.EOF)) {
                    LOGGER.info("finish to write dump file.");
                    close();
                    return;
                }

                for (int i = 0, srcLength = content.length; i < srcLength; i++) {
                    String s = content[i] != null ? content[i] : "";
                    this.bufferedWriter.write(s);
                    if (i != srcLength - 1) {
                        this.bufferedWriter.write(',');
                    }
                }
                this.bufferedWriter.newLine();
            } catch (Exception | Error e) {
                LOGGER.warn("split writer error,", e);
                this.errorFlag.compareAndSet(false, true);
                this.errorMsgList.add(new ErrorMsg("split " + shardingNode + " writer error[exit]", e.getMessage()));
                this.bufferedWriter.close();
            }
        };
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        try {
            this.disruptor = new Disruptor(factory, config.getDisruptorBufferSize(), new ThreadFactoryBuilder().setNameFormat(shardingNode + "-splitLoadWriter").build(),
                    ProducerType.SINGLE, strategy);
        } catch (IllegalArgumentException e) {
            throw new DumpException("The value of -w needs to be a power of 2");
        }
        this.disruptor.handleEventsWith(handler);
        this.disruptor.setDefaultExceptionHandler(new SplitWriterExceptionHandler());
        this.disruptor.start();
        this.started.compareAndSet(false, true);
    }

    public void write(String[] row, AtomicBoolean error) {
        while (!this.started.get() && !error.get()) {
            //wait disruptor init
            LockSupport.parkNanos(1000);
        }
        if (error.get()) {
            //error
            return;
        }
        disruptor.getRingBuffer().publishEvent(translator, row);
    }

    public void close() throws IOException {
        this.bufferedWriter.close();
        this.nodeCount.decrementAndGet();
    }

    public void shutdown() {
        if (this.disruptor != null) {
            this.disruptor.shutdown();
        }
    }

    public static class Element {

        private String[] value;

        Element() {
        }

        public String[] getValue() {
            return value;
        }

        public void setValue(String[] value) {
            this.value = value;
        }
    }


    // exception
    public static final class SplitWriterExceptionHandler implements ExceptionHandler {

        public SplitWriterExceptionHandler() {
        }

        @Override
        public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.warn("Exception processing: {} {} ,exception：", sequence, event, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOGGER.error("Exception during onStart for split disruptor ,exception：", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("Exception during onShutdown for split disruptor ,exception：", ex);
        }
    }
}
