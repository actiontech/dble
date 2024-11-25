/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;

import com.oceanbase.obsharding_d.btrace.provider.GeneralProvider;
import com.oceanbase.obsharding_d.log.RotateLogStore;
import com.oceanbase.obsharding_d.server.status.GeneralLog;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeneralLogDisruptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLogDisruptor.class);

    private int ringBufferSize;
    private Disruptor<GeneralLogEvent> disruptor;
    private EventFactory<GeneralLogEvent> eventFactory;
    private EventTranslatorTwoArg<GeneralLogEvent, GeneralLogEntry, RotateLogStore.LogFileManager> translator;
    private RotateLogStore.LogFileManager fileManager;

    public GeneralLogDisruptor(RotateLogStore.LogFileManager fileManager, int ringBufferSize) {
        this.fileManager = fileManager;
        this.ringBufferSize = ringBufferSize;
    }

    public void start() {
        GeneralProvider.getGeneralLogQueueSize(ringBufferSize);
        this.fileManager.init();
        this.eventFactory = EVENTFACTORY;
        this.translator = TRANSLATOR;
        this.disruptor = new Disruptor<>(eventFactory, ringBufferSize, new ThreadFactoryBuilder().setNameFormat("GENERAL-%d").build());
        //this.disruptor = new Disruptor<>(eventFactory, ringBufferSize, new ThreadFactoryBuilder().setNameFormat("GENERAL-%d").build(), ProducerType.MULTI, new LiteBlockingWaitStrategy());
        this.disruptor.handleEventsWith(new GeneralLogFilePathChangeEventHandler()).then(new GeneralLogEventHandler());
        this.disruptor.setDefaultExceptionHandler(new GeneralLogExceptionHandler());
        this.disruptor.start();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("disruptor start success");
        }
    }

    public boolean stop() {
        final Disruptor<GeneralLogEvent> temp = disruptor;
        if (temp == null) {
            return true;
        }
        disruptor = null;
        temp.shutdown();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("disruptor shutdown success");
        }
        return true;
    }

    public boolean tryEnqueue(final GeneralLogEntry generalLogEntry) {
        return disruptor.getRingBuffer().tryPublishEvent(this.translator, generalLogEntry, fileManager);
    }

    public void justEnqueue(final GeneralLogEntry generalLogEntry) {
        LOGGER.info("disruptor queue is full, current thread will enters the blocking state");
        disruptor.getRingBuffer().publishEvent(this.translator, generalLogEntry, fileManager);
        LOGGER.info("current thread exits the blocking state");
    }

    private static final EventFactory<GeneralLogEvent> EVENTFACTORY = new EventFactory<GeneralLogEvent>() {
        @Override
        public GeneralLogEvent newInstance() {
            return new GeneralLogEvent();
        }
    };

    // translator
    private static final EventTranslatorTwoArg<GeneralLogEvent, GeneralLogEntry, RotateLogStore.LogFileManager> TRANSLATOR = new EventTranslatorTwoArg<GeneralLogEvent, GeneralLogEntry, RotateLogStore.LogFileManager>() {
        @Override
        public void translateTo(GeneralLogEvent event, long l, GeneralLogEntry entry, RotateLogStore.LogFileManager manager) {
            event.setLogEntry(entry);
            event.setManager(manager);
        }
    };

    // log event
    public static final class GeneralLogEvent {
        private GeneralLogEntry logEntry;
        private RotateLogStore.LogFileManager manager;

        public void setManager(RotateLogStore.LogFileManager manager) {
            this.manager = manager;
        }

        public RotateLogStore.LogFileManager getManager() {
            return manager;
        }

        public void setLogEntry(GeneralLogEntry logEntry) {
            this.logEntry = logEntry;
        }

        public GeneralLogEntry getLogEntry() {
            return logEntry;
        }

        public void appendTo(boolean isEndOfBatch) throws IOException {
            manager.append(logEntry, isEndOfBatch);
        }

        public void clear() {
            logEntry = null;
        }
    }

    // consumption
    public static final class GeneralLogFilePathChangeEventHandler implements EventHandler<GeneralLogEvent> {
        @Override
        public void onEvent(GeneralLogEvent generalLogEvent, long sequence, boolean endOfBatch) {
            if (!generalLogEvent.getManager().getFileName().equals(GeneralLog.getInstance().getGeneralLogFile())) {
                generalLogEvent.getManager().setFileName(GeneralLog.getInstance().getGeneralLogFile());
                generalLogEvent.getManager().reset();
            }
        }
    }

    // consumption
    public static final class GeneralLogEventHandler implements EventHandler<GeneralLogEvent> {
        @Override
        public void onEvent(GeneralLogEvent generalLogEvent, long sequence, boolean endOfBatch) throws Exception {
            generalLogEvent.appendTo(endOfBatch);
            generalLogEvent.clear();
        }
    }

    // exception
    public static final class GeneralLogExceptionHandler implements ExceptionHandler {
        @Override
        public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.warn("Exception processing: {} {} ,exception：{}", sequence, event, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOGGER.error("Exception during onStart for general log's disruptor ,exception：{}", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("Exception during onShutdown for general log's disruptor ,exception：{}", ex);
        }
    }

}
