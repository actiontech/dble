package com.actiontech.dble.meta;

import com.actiontech.dble.cluster.values.ConfStatus;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2019/7/15.
 */
public final class ReloadManager {

    private static final ReloadManager RELOAD_INSTANCE = new ReloadManager();
    private final AtomicInteger sequence = new AtomicInteger(0);
    private volatile ReloadStatus status = null;

    private ReloadManager() {

    }

    public ReloadStatus getStatus() {
        return status;
    }

    public static boolean checkCanRelease() {
        if (RELOAD_INSTANCE.status != null) {
            return RELOAD_INSTANCE.status.checkCanRelease();
        }
        return false;
    }


    public static boolean interruptReload() {
        if (RELOAD_INSTANCE.status != null) {
            return RELOAD_INSTANCE.status.interruputed();
        }
        return false;
    }

    public static ReloadManager getReloadInstance() {
        return RELOAD_INSTANCE;
    }

    public int nextId() {
        return sequence.getAndIncrement();
    }

    public boolean isReloadInterrupted() {
        return status == null ? false : status.isReloadInterrupted();
    }

    public static boolean startReload(String triggerType, ConfStatus.Status reloadType) {
        if (RELOAD_INSTANCE.status != null) {
            if (RELOAD_INSTANCE.status.isFinished()) {
                RELOAD_INSTANCE.status = new ReloadStatus(triggerType, new ConfStatus(reloadType, null));
            } else {
                return false;
            }
        } else {
            RELOAD_INSTANCE.status = new ReloadStatus(triggerType, new ConfStatus(reloadType, null));
        }
        return true;
    }

    public static boolean startReload(String triggerType, ConfStatus confStatus) {
        if (RELOAD_INSTANCE.status != null) {
            if (RELOAD_INSTANCE.status.isFinished()) {
                RELOAD_INSTANCE.status = new ReloadStatus(triggerType, confStatus);
            } else {
                return false;
            }
        } else {
            RELOAD_INSTANCE.status = new ReloadStatus(triggerType, confStatus);
        }
        return true;
    }

    public static void metaReload() {
        if (RELOAD_INSTANCE.status != null) {
            RELOAD_INSTANCE.status.metaReload();
        }
    }

    public static void waitingOthers() {
        if (RELOAD_INSTANCE.status != null) {
            RELOAD_INSTANCE.status.waitingOthers();
        }
    }

    public static void reloadFinish() {
        if (RELOAD_INSTANCE.status != null) {
            RELOAD_INSTANCE.status.reloadFinish();
        }
    }

}
