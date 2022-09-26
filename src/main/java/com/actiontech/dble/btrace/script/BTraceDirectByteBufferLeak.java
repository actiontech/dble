/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.*;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

@BTrace(unsafe = true)
public class BTraceDirectByteBufferLeak {

    private BTraceDirectByteBufferLeak() {
    }

    @OnMethod(
            clazz = "com.actiontech.dble.buffer.DirectByteBufferPool",
            method = "recycle",
            location = @Location(Kind.RETURN)
    )
    public static void recycle(@ProbeClassName String pcn, @ProbeMethodName String pmn, ByteBuffer buf) {
        String threadName = BTraceUtils.currentThread().getName();
        if (!threadName.contains("writeTo")) {
            String js = BTraceUtils.jstackStr(15);
            if (!js.contains("heartbeat") && !js.contains("XAAnalysisHandler")) {
                BTraceUtils.println(threadName);
                if (buf.isDirect()) {
                    BTraceUtils.println("r:" + ((DirectBuffer) buf).address());
                }
                BTraceUtils.println(js);
            }
        }
    }

    @OnMethod(
            clazz = "com.actiontech.dble.buffer.DirectByteBufferPool",
            method = "allocate",
            location = @Location(Kind.RETURN)
    )
    public static void allocate(@Return ByteBuffer buf) {
        String threadName = BTraceUtils.currentThread().getName();
        if (!threadName.contains("writeTo")) {
            String js = BTraceUtils.jstackStr(15);
            if (!js.contains("heartbeat") && !js.contains("XAAnalysisHandler")) {
                BTraceUtils.println(threadName);
                if (buf.isDirect()) {
                    BTraceUtils.println("a:" + ((DirectBuffer) buf).address());
                }
                BTraceUtils.println(js);
            }
        }
    }

}
