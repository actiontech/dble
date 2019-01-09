package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.Profiler;
import com.sun.btrace.annotations.*;

import java.util.Map;

import static com.sun.btrace.BTraceUtils.timeNanos;

@BTrace
public final class BtraceComplexCostTime {

    private BtraceComplexCostTime() {

    }

    private static Map<Long, Long> records = BTraceUtils.Collections.newHashMap();

    @Property
    static Profiler profiler = BTraceUtils.Profiling.newProfiler();

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.CostTimeProvider",
            method = "beginRequest"
    )
    public static void beginRequest(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        BTraceUtils.Collections.put(records, arg, timeNanos());
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.CostTimeProvider",
            method = "startProcess"
    )
    public static void startProcess(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->1.startProcess", duration);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.CostTimeProvider",
            method = "endParse"
    )
    public static void endParse(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->2.endParse", duration);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ComplexQueryProvider",
            method = "endRoute"
    )
    public static void endRoute(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->3.endRoute", duration);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ComplexQueryProvider",
            method = "endComplexExecute"
    )
    public static void endExecute(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->4.endComplexExecute", duration);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.CostTimeProvider",
            method = "resFromBack"
    )
    public static void firstBackendResponse(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->5.firstBackendResponse", duration);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ComplexQueryProvider",
            method = "endComplexExecute"
    )
    public static void firstBackendEof(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->6.firstBackendEof", duration);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.CostTimeProvider",
            method = "beginResponse"
    )
    public static void complexOutput(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod, long arg) {
        Long ts = BTraceUtils.Collections.get(records, arg);
        if (ts == null) {
            return;
        }
        long duration = timeNanos() - ts;
        BTraceUtils.Profiling.recordExit(profiler, "request->7.complexOutput", duration);
    }


    @OnTimer(4000)
    public static void print() {
        BTraceUtils.Profiling.printSnapshot("profiling:", profiler);
    }
}
