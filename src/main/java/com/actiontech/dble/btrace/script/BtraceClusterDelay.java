package com.actiontech.dble.btrace.script;

import com.sun.btrace.BTraceUtils;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.ProbeClassName;
import com.sun.btrace.annotations.ProbeMethodName;


/**
 * Created by szf on 2018/3/28.
 */
@BTrace(unsafe = true)
public final class BtraceClusterDelay {


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterGetLock"
    )
    public static void delayAfterGetLock(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterGetLock ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterViewSetKey"
    )
    public static void delayAfterViewSetKey(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterViewSetKey ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterViewNotic"
    )
    public static void delayAfterViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterViewNotic ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayWhenReponseViewNotic"
    )
    public static void delayWhenReponseViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayWhenReponseViewNotic ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeReponseGetView"
    )
    public static void delayBeforeReponseGetView(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeReponseGetView ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeReponseView"
    )
    public static void delayBeforeReponseView(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeReponseView ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "beforeDeleteViewNotic"
    )
    public static void beforeDeleteViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterGetLock ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "beforeReleaseViewLock"
    )
    public static void beforeReleaseViewLock(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into beforeReleaseViewLock ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlLockMeta"
    )
    public static void delayAfterDdlLockMeta(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterDdlLockMeta ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlExecuted"
    )
    public static void delayAfterDdlExecuted(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterDdlExecuted ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlNotice"
    )
    public static void delayBeforeDdlNotice(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeDdlNotice ");
        Thread.sleep(10000L);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlNotice"
    )
    public static void delayAfterDdlNotice(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterDdlNotice ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlNoticeDeleted"
    )
    public static void delayBeforeDdlNoticeDeleted(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeDdlNoticeDeleted ");
        Thread.sleep(10000L);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlLockRelease"
    )
    public static void delayBeforeDdlLockRelease(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeDdlLockRelease ");
        Thread.sleep(10000L);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterGetDdlNotice"
    )
    public static void delayAfterGetDdlNotice(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayAfterGetDdlNotice ");
        Thread.sleep(10000L);
    }

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeUpdateMeta"
    )
    public static void delayBeforeUpdateMeta(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeUpdateMeta ");
        Thread.sleep(10000L);
    }


    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlLockRelease"
    )
    public static void delayBeforeDdlResponse(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.println("get into delayBeforeDdlResponse ");
        Thread.sleep(10000L);
    }

}
