package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.OnMethod;

@BTrace(unsafe = true)
public final class BtraceXaDelay {

    private BtraceXaDelay() {

    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "delayBeforeXaStart"
    )
    public static void delayBeforeXaStart(String rrnName, String xaId) throws Exception {
        BTraceUtils.println("before xa start " + xaId + " in " + rrnName);
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "delayBeforeXaEnd"
    )
    public static void delayBeforeXaEnd(String rrnName, String xaId) throws Exception {
        BTraceUtils.println("before xa end " + xaId + " in " + rrnName);
        Thread.sleep(10000L);

    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "delayBeforeXaPrepare"
    )
    public static void delayBeforeXaPrepare(String rrnName, String xaId) throws Exception {
        BTraceUtils.println("before xa prepare " + xaId + " in " + rrnName);
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "delayBeforeXaCommit"
    )
    public static void delayBeforeXaCommit(String rrnName, String xaId) throws Exception {
        BTraceUtils.println("before xa commit " + xaId + " in " + rrnName);
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "delayBeforeXaRollback"
    )
    public static void delayBeforeXaRollback(String rrnName, String xaId) throws Exception {
        BTraceUtils.println("before xa rollback " + xaId + " in " + rrnName);
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "beforeAddXaToQueue"
    )
    public static void beforeAddXaToQueue(int count, String xaId) throws Exception {
        BTraceUtils.println("before add xa " + xaId + " to queue in " + count + " time.");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "afterAddXaToQueue"
    )
    public static void afterAddXaToQueue(int count, String xaId) throws Exception {
        BTraceUtils.println("after add xa " + xaId + " to queue in " + count + " time.");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.XaDelayProvider",
            method = "beforeInnerRetry"
    )
    public static void beforeInnerRetry(int count, String xaId) throws Exception {
        BTraceUtils.println("before inner retry " + xaId + " in " + count + " time.");
        Thread.sleep(10000L);
    }

}
