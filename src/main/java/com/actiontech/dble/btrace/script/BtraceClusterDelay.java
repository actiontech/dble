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
        BTraceUtils.print("get into delayAfterGetLock ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);

    }
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterViewSetKey"
    )
    public static void delayAfterViewSetKey(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterViewSetKey ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterViewNotic"
    )
    public static void delayAfterViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterViewNotic ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayWhenReponseViewNotic"
    )
    public static void delayWhenReponseViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayWhenReponseViewNotic ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeReponseGetView"
    )
    public static void delayBeforeReponseGetView(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayBeforeReponseGetView ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeReponseView"
    )
    public static void delayBeforeReponseView(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayBeforeReponseView ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "beforeDeleteViewNotic"
    )
    public static void beforeDeleteViewNotic(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterGetLock ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "beforeReleaseViewLock"
    )
    public static void beforeReleaseViewLock(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into beforeReleaseViewLock ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlLockMeta"
    )
    public static void delayAfterDdlLockMeta(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterDdlLockMeta ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlExecuted"
    )
    public static void delayAfterDdlExecuted(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterDdlExecuted ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlNotice"
    )
    public static void delayBeforeDdlNotice(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayBeforeDdlNotice ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*

    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayAfterDdlNotice"
    )
    public static void delayAfterDdlNotice(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayAfterDdlNotice ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/
/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlNoticeDeleted"
    )
    public static void delayBeforeDdlNoticeDeleted(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayBeforeDdlNoticeDeleted ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/

/*
    @OnMethod(
            clazz = "com.actiontech.dble.btrace.provider.ClusterDelayProvider",
            method = "delayBeforeDdlLockRelease"
    )
    public static void delayBeforeDdlLockRelease(@ProbeClassName String probeClass, @ProbeMethodName String probeMethod) throws Exception {
        BTraceUtils.print("get into delayBeforeDdlLockRelease ");
        BTraceUtils.print(" for order __________________________ ");
        Thread.sleep( 10000L);
    }*/


}
