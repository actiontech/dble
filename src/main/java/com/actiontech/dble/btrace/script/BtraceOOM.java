package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.OnMethod;


@BTrace(unsafe = true)
public final class BtraceOOM {
    private BtraceOOM() {
    }

    @OnMethod(
            clazz = "com.actiontech.dble.backend.mysql.nio.MySQLConnection",
            method = "register"
    )
    public static void register() throws Exception {
        BTraceUtils.println("delay for register test");
        Thread.sleep(15000L);
        BTraceUtils.println("...................");
    }
}
