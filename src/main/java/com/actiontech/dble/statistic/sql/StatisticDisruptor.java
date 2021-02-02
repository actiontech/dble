package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.btrace.provider.StatisticProvider;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.handler.StatisticDataHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticDisruptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticDisruptor.class);

    private Disruptor<StatisticEvent> disruptor;
    private EventFactory<StatisticEvent> eventFactory;
    private EventTranslatorOneArg<StatisticEvent, StatisticEntry> translator;

    public StatisticDisruptor(int ringBufferSize, StatisticDataHandler... dataHandler) {
        StatisticProvider.getStatisticQueueSize(ringBufferSize);
        eventFactory = EVENTFACTORY;
        translator = TRANSLATOR;
        disruptor = new Disruptor<>(eventFactory, ringBufferSize, new ThreadFactoryBuilder().setNameFormat("STATISTIC-%d").build(), ProducerType.MULTI, new LiteBlockingWaitStrategy());
        disruptor.handleEventsWith(dataHandler);
        disruptor.setDefaultExceptionHandler(new StatisticExceptionHandler());
        disruptor.start();
    }

    public boolean stop() {
        final Disruptor<StatisticEvent> temp = disruptor;
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

    public boolean push(final StatisticEntry entry) {
        boolean succ = disruptor.getRingBuffer().tryPublishEvent(translator, entry);
        if (!succ) {
            disruptor.getRingBuffer().publishEvent(translator, entry);
        }
        return true;
    }

    private static final EventFactory<StatisticEvent> EVENTFACTORY = new EventFactory<StatisticEvent>() {
        @Override
        public StatisticEvent newInstance() {
            return new StatisticEvent();
        }
    };

    private static final EventTranslatorOneArg<StatisticEvent, StatisticEntry> TRANSLATOR = new EventTranslatorOneArg<StatisticEvent, StatisticEntry>() {
        @Override
        public void translateTo(StatisticEvent statisticEvent, long l, StatisticEntry entry) {
            statisticEvent.setEntry(entry);
        }
    };

    // exception
    public static final class StatisticExceptionHandler implements ExceptionHandler {
        @Override
        public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.warn("Exception processing: {} {} ,exception：{}", sequence, event, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOGGER.error("Exception during onStart for statistic's disruptor ,exception：{}", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("Exception during onShutdown for statistic's disruptor ,exception：{}", ex);
        }
    }
}
