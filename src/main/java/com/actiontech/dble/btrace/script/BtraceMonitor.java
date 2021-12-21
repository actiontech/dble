/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */


import com.sun.btrace.AnyType;
import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.*;


@BTrace(unsafe = true)
public final class BtraceMonitor {

    private BtraceMonitor() {

    }

    @OnMethod(
            clazz = "com.actiontech.dble.buffer.DirectByteBufferPool",
            method = "allocate", location = @Location(Kind.RETURN)
    )
    public static void allocate(AnyType[] args, @Return
            Object callbackData, @ProbeMethodName String probeMethod) throws Exception {

        String type = "allocate";

        printData(callbackData, type, args);
    }

    private static void printData(Object data, String type, Object[] args) {
        if (args.length == 0 && "allocate".equals(type)) {
            //ignore
            return;
        }
        synchronized (BtraceMonitor.class) {

            BTraceUtils.println(">>>");
            BTraceUtils.println("{type:\"" + type + "\",time:\"" + currentTime() + "\"} ");
            BTraceUtils.printFields(data);
            try {
                throw new RuntimeException("debugger");
            } catch (Exception e) {
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    BTraceUtils.println(stackTraceElement);
                }
            }
            BTraceUtils.println("<<<");
        }
    }


    @OnMethod(
            clazz = "com.actiontech.dble.buffer.DirectByteBufferPool",
            method = "recycle"
    )
    public static void recycle(AnyType[] args, @ProbeMethodName String probeMethod) throws Exception {
        String type = "recycle";
        printData(args[0], type, args);
    }

    private static String currentTime() {
        return java.time.LocalDateTime.now().toString();
    }


}
