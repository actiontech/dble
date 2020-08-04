package com.actiontech.dble.singleton;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.AbstractService;
import com.google.common.collect.ImmutableMap;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.metrics.Metrics;
import io.jaegertracing.internal.metrics.NoopMetricsFactory;
import io.jaegertracing.internal.reporters.CompositeReporter;
import io.jaegertracing.internal.reporters.LoggingReporter;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.jaegertracing.internal.samplers.ProbabilisticSampler;
import io.jaegertracing.thrift.internal.senders.HttpSender;
import io.opentracing.Scope;
import io.opentracing.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2020/5/9.
 */
public final class TraceManager {
    private static final TraceManager INSTANCE = new TraceManager();
    private final JaegerTracer tracer;
    private final Map<AbstractService, List<TraceObject>> connectionTracerMap = new ConcurrentHashMap<>();
    ThreadLocal<AbstractService> seriveces = new InheritableThreadLocal<>();


    private TraceManager() {
        final String endPoint = SystemConfig.getInstance().getTraceEndPoint();
        if (endPoint != null) {
            final CompositeReporter compositeReporter = new CompositeReporter(
                    new RemoteReporter.Builder().
                            withSender(new HttpSender.Builder(endPoint).build()).
                            build(),
                    new LoggingReporter()
            );

            final Metrics metrics = new Metrics(new NoopMetricsFactory());

            JaegerTracer.Builder builder = new JaegerTracer.Builder("DBLE").withReporter(compositeReporter).
                    withMetrics(metrics).
                    withExpandExceptionLogs().
                    withSampler(new ProbabilisticSampler(0.5));

            tracer = builder.build();
        } else {
            tracer = null;
        }
    }

    public static JaegerTracer getTracer() {
        return INSTANCE.tracer;
    }


    public static void sessionStart(AbstractService service, String traceMessage) {
        if (INSTANCE.tracer != null) {
            if (INSTANCE.connectionTracerMap.get(service) == null) {
                TraceObject traceObject = spanCreateActive(traceMessage, false, null, service);
                traceObject.span.log(ImmutableMap.of("service detail", service.toBriefString()));
                List<TraceObject> spanList = new ArrayList<>();
                spanList.add(traceObject);
                INSTANCE.connectionTracerMap.put(service, spanList);
            }
        }
    }


    public static void sessionFinish(AbstractService service) {
        if (INSTANCE.tracer != null) {
            TraceObject object = popServiceSpan(service, true);
            while (object != null) {
                TraceManager.finishSpan(object);
                object = popServiceSpan(service, true);
            }
            INSTANCE.connectionTracerMap.remove(service);
        }
    }

    public static void queryFinish(AbstractService service) {
        if (INSTANCE.tracer != null) {
            TraceObject object = popServiceSpan(service, true);
            TraceObject object2 = popServiceSpan(service, false);
            while (object != null && object2 != null) {
                TraceManager.finishSpan(object);
                object = popServiceSpan(service, true);
                object2 = popServiceSpan(service, false);
            }
        }
    }

    public static TraceObject serviceTrace(AbstractService service, String traceMessage) {
        if (INSTANCE.tracer != null) {
            List<TraceObject> spanList = INSTANCE.connectionTracerMap.get(service);
            INSTANCE.seriveces.set(service);
            if (spanList != null) {
                TraceObject fSpan = popServiceSpan(service, false);
                TraceObject span = spanCreateActive(traceMessage, true, fSpan, service);
                spanList.add(span);
                return span;
            }
        }
        return null;
    }

    public static AbstractService getThreadService() {
        if (INSTANCE.tracer != null) {
            return INSTANCE.seriveces.get();
        } else {
            return null;
        }
    }

    public static TraceObject crossThread(AbstractService service, String traceMessage, AbstractService fService) {
        if (INSTANCE.tracer != null) {
            if (fService != null) {
                TraceObject fSpan = popServiceSpan(fService, false);
                TraceObject span = spanCreateActive(traceMessage, true, fSpan, service);
                List<TraceObject> spanList = new ArrayList<>();
                spanList.add(span);
                INSTANCE.connectionTracerMap.put(service, spanList);
                return span;
            }
        }
        return null;
    }

    public static TraceObject threadTrace(String traceMessage) {
        if (INSTANCE.tracer != null) {
            TraceObject to = spanCreateActive(traceMessage, true, null, null);
            return to;
        }
        return null;
    }

    public static void finishSpan(AbstractService service, TraceManager.TraceObject to) {
        if (INSTANCE.tracer != null) {
            TraceObject traceObject = popServiceSpan(service, false);
            if (to != null && traceObject == to) {
                traceObject = popServiceSpan(service, true);
                traceObject.finish();
                return;
            }
        }
    }

    public static void finishSpan(TraceObject object) {
        if (INSTANCE.tracer != null) {
            if (object != null) {
                object.finish();
            }
        }
    }

    private static TraceObject popServiceSpan(AbstractService service, boolean remove) {
        List<TraceObject> spanList = INSTANCE.connectionTracerMap.get(service);
        if (spanList != null && spanList.size() != 0) {
            synchronized (spanList) {
                TraceObject to = spanList.get(spanList.size() - 1);
                if (remove) {
                    spanList.remove(spanList.size() - 1);
                }
                return to;
            }
        }
        return null;
    }

    private static TraceObject spanCreateActive(String message, boolean active, TraceObject fspan, AbstractService service) {
        Span span = null;
        Scope scope = null;
        if (fspan != null) {
            span = TraceManager.getTracer().buildSpan(message).asChildOf(fspan.span).start();
        } else {
            span = TraceManager.getTracer().buildSpan(message).start();
        }
        if (active) {
            scope = TraceManager.getTracer().scopeManager().activate(span);
        }
        return new TraceObject(span, scope, service);
    }

    public static void log(Map<String, ?> var1, TraceObject traceObject) {
        if (traceObject != null) {
            traceObject.log(var1);
        }
    }

    public static class TraceObject {
        final Scope scope;
        final Span span;
        final AbstractService service;

        TraceObject(Span spanx, Scope scopex, AbstractService servicex) {
            scope = scopex;
            span = spanx;
            service = servicex;
        }

        public void finish() {
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.finish();
            }
        }

        public void log(Map<String, ?> var1) {
            if (span != null) {
                span.log(var1);
            }
        }
    }
}
