/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.config.ProblemReporter;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.util.StartProblemReporter;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.metrics.Metrics;
import io.jaegertracing.internal.metrics.NoopMetricsFactory;
import io.jaegertracing.internal.reporters.CompositeReporter;
import io.jaegertracing.internal.reporters.LoggingReporter;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.jaegertracing.internal.samplers.ProbabilisticSampler;
import io.jaegertracing.internal.samplers.RateLimitingSampler;
import io.jaegertracing.spi.Sampler;
import io.jaegertracing.thrift.internal.senders.HttpSender;
import io.opentracing.Scope;
import io.opentracing.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2020/5/9.
 */
public final class TraceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceManager.class);
    private static final TraceManager INSTANCE = new TraceManager();

    private final ProblemReporter problemReporter = StartProblemReporter.getInstance();
    private JaegerTracer tracer = null;
    private String samplerType = "";
    private String samplerParam = "";

    private final Map<AbstractService, List<TraceObject>> connectionTracerMap = new ConcurrentHashMap<>();
    private final ThreadLocal<AbstractService> services = new InheritableThreadLocal<>();

    private TraceManager() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) return;

        final String endPoint = SystemConfig.getInstance().getTraceEndPoint();
        if (endPoint == null) return;

        Object[] result = buildSampler(SystemConfig.getInstance().getTraceSamplerType(), SystemConfig.getInstance().getTraceSamplerParam());
        if (result == null) return;

        Sampler sampler = (Sampler) result[2];
        LOGGER.info("Trace Jaeger use {}", sampler);

        final CompositeReporter compositeReporter = new CompositeReporter(
                new RemoteReporter.Builder().
                        withSender(new HttpSender.Builder(endPoint).build()).
                        build(),
                new LoggingReporter()
        );

        final Metrics metrics = new Metrics(new NoopMetricsFactory());

        JaegerTracer.Builder builder = new JaegerTracer.Builder("OBsharding-D").withReporter(compositeReporter).
                withMetrics(metrics).
                withExpandExceptionLogs().
                withSampler(sampler);

        tracer = builder.build();
        samplerType = String.valueOf(result[0]);
        samplerParam = String.valueOf(result[1]);
    }

    private Object[] buildSampler(String samplerType0, String samplerParam0) {
        if (StringUtil.isBlank(samplerType0)) {
            return new Object[]{"const", "1.0", new ConstSampler(true)};
        }

        switch (samplerType0.toLowerCase()) {
            case "const":
                if (StringUtil.isBlank(samplerParam0)) {
                    samplerParam0 = "1.0";
                }
                if (StringUtil.isDoubleOrFloat(samplerParam0)) {
                    double num = Double.parseDouble(samplerParam0);
                    if (num == 0.0D) {
                        return new Object[]{"const", "0.0", new ConstSampler(false)};
                    } else if (num == 1.0D) {
                        return new Object[]{"const", "1.0", new ConstSampler(true)};
                    }
                }
                problemReporter.error("The [const] sampling param must be equal 0.0 or 1.0");
                break;
            case "probabilistic":
                if (StringUtil.isBlank(samplerParam0)) {
                    samplerParam0 = "0.1";
                }
                if (StringUtil.isDoubleOrFloat(samplerParam0)) {
                    double num = Double.parseDouble(samplerParam0);
                    if (!(num < 0.0D) && !(num > 1.0D)) {
                        return new Object[]{"probabilistic", num, new ProbabilisticSampler(num)};
                    }
                }
                problemReporter.error("The [probabilistic] sampling param must be greater than 0.0 and less than 1.0");
                break;
            case "ratelimiting":
                if (StringUtil.isBlank(samplerParam0)) {
                    samplerParam0 = "1.0";
                }
                if (StringUtil.isDoubleOrFloat(samplerParam0)) {
                    double num = Double.parseDouble(samplerParam0);
                    return new Object[]{"ratelimiting", num, new RateLimitingSampler(num)};
                }
                problemReporter.error("The [ratelimiting] sampling param must be a number");
                break;
            case "remote":
                problemReporter.error("The [ratelimiting] sampling is not supported");
                return null;
            default:
                problemReporter.error("There is no a sampling, select the supported sampling from [const, probabilistic, ratelimiting]");
                return null;
        }
        return null;
    }

    public static JaegerTracer getTracer() {
        return INSTANCE.tracer;
    }

    public static boolean isEnable() {
        return INSTANCE.tracer != null;
    }

    public static String getSamplerType() {
        return INSTANCE.samplerType;
    }

    public static String getSamplerParam() {
        return INSTANCE.samplerParam;
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
            INSTANCE.services.set(service);
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
            return INSTANCE.services.get();
        } else {
            return null;
        }
    }

    public static TraceObject crossThread(AbstractService service, String traceMessage, AbstractService fService) {
        if (INSTANCE.tracer != null) {
            if (fService != null && !fService.isFakeClosed()) {
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
