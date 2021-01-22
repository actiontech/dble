package com.actiontech.dble.statistic.backend;

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

    private Disruptor<Event> disruptor;
    private EventFactory<Event> eventFactory;
    private EventTranslatorOneArg<Event, StatisticEntry> translator;

    public StatisticDisruptor(int ringBufferSize, StatisticDataHandler... dataHandler) {
        eventFactory = EVENTFACTORY;
        translator = TRANSLATOR;
        disruptor = new Disruptor<>(eventFactory, ringBufferSize, new ThreadFactoryBuilder().setNameFormat("STATISTIC-%d").build(), ProducerType.MULTI, new LiteBlockingWaitStrategy());
        disruptor.handleEventsWith(dataHandler);
        disruptor.setDefaultExceptionHandler(new StatisticExceptionHandler());
        disruptor.start();
    }

    public boolean stop() {
        final Disruptor<Event> temp = disruptor;
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

    private static final EventFactory<Event> EVENTFACTORY = new EventFactory<Event>() {
        @Override
        public Event newInstance() {
            return new Event();
        }
    };

    private static final EventTranslatorOneArg<Event, StatisticEntry> TRANSLATOR = new EventTranslatorOneArg<Event, StatisticEntry>() {
        @Override
        public void translateTo(Event event, long l, StatisticEntry entry) {
            event.setEntry(entry);
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
