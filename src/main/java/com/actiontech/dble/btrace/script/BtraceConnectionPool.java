package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.OnMethod;

@BTrace(unsafe = true)
public final class BtraceConnectionPool {

    private BtraceConnectionPool() {
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ConnectionPoolProvider",
            method = "freshConnGetRealodLocekAfter"
    )
    public static void freshConnGetRealodLocekAfter() throws Exception {
        BTraceUtils.println("------- start sleep freshConnGetRealodLocekAfter -------");
        Thread.sleep(30000L);
        BTraceUtils.println("------- end sleep freshConnGetRealodLocekAfter -------");
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ConnectionPoolProvider",
            method = "stopConnGetFrenshLocekAfter"
    )
    public static void stopConnGetFrenshLocekAfter() throws Exception {
        BTraceUtils.println("------- start sleep stopConnGetFrenshLocekAfter -------");
        Thread.sleep(30000L);
        BTraceUtils.println("------- end sleep stopConnGetFrenshLocekAfter -------");

    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ConnectionPoolProvider",
            method = "getConnGetFrenshLocekAfter"
    )
    public static void getConnGetFrenshLocekAfter() throws Exception {
        BTraceUtils.println("------- start sleep getConnGetFrenshLocekAfter -------");
        Thread.sleep(30000L);
        BTraceUtils.println("------- end sleep getConnGetFrenshLocekAfter -------");
    }
}
