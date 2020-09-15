package com.actiontech.dble.meta;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.meta.table.ServerMetaHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2019/7/15.
 */
public class ReloadStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadStatus.class);
    //4 types of RELOAD_STATUS start with SELF_RELOAD
    public static final String RELOAD_STATUS_NONE = "NOT_RELOADING";
    public static final String RELOAD_STATUS_SELF_RELOAD = "SELF_RELOAD";
    public static final String RELOAD_STATUS_META_RELOAD = "META_RELOAD";
    public static final String RELOAD_STATUS_WAITING_OTHERS = "WAITING_OTHERS";

    //2 types of TRIGGER_TYPE
    public static final String TRIGGER_TYPE_CLUSTER = "CLUSTER_NOTIFY";
    public static final String TRIGGER_TYPE_COMMAND = "LOCAL_COMMAND";

    public static final String RELOAD_END_NORMAL = "RELOAD_END";
    public static final String RELOAD_INTERRUPUTED = "INTERRUPUTED";

    private final int id;
    private final String clusterType;
    private volatile String status;
    private final String triggerType;
    private final long lastReloadStart;
    private volatile long lastReloadEnd = 0;
    private final AtomicBoolean interruputed = new AtomicBoolean(false);
    private volatile ServerMetaHandler reloadHandler;

    private final ConfStatus confStatus;

    public ReloadStatus(String triggerType, ConfStatus confStatus) {
        clusterType = ClusterGeneralConfig.getInstance().getClusterType();
        id = ReloadManager.getReloadInstance().nextId();
        this.triggerType = triggerType;
        status = RELOAD_STATUS_SELF_RELOAD;
        lastReloadStart = System.currentTimeMillis();
        this.confStatus = confStatus;
        LOGGER.info(this.getLogStage() + "_____________________reload start________" + id + "__" + confStatus.getStatusAExtraInfo());
    }

    public void register(ServerMetaHandler handler) {
        reloadHandler = handler;
    }

    public void metaReload() {
        this.status = RELOAD_STATUS_META_RELOAD;
        LOGGER.info(this.getLogStage() + "_____________________meta reload start________" + id + "__" + confStatus.getStatus());
    }

    public void waitingOthers() {
        this.status = RELOAD_STATUS_WAITING_OTHERS;
        LOGGER.info(this.getLogStage() + "_____________________waiting others___________" + id + "__" + confStatus.getStatus());
    }

    public void reloadFinish() {
        this.status = RELOAD_STATUS_NONE;
        this.lastReloadEnd = System.currentTimeMillis();
        LOGGER.info(this.getLogStage() + "_____________________reload finished___________" + id + "__" + confStatus.getStatusAExtraInfo());
    }

    public boolean isFinished() {
        return status.equals(RELOAD_STATUS_NONE);
    }

    public boolean checkCanRelease() {
        return status.equals(RELOAD_STATUS_META_RELOAD);
    }

    public boolean interruputed() {
        if (interruputed.compareAndSet(false, true)) {
            reloadHandler.release();
            return true;
        }
        return false;
    }

    public boolean isReloadInterrupted() {
        return interruputed.get();
    }

    public String getLogStage() {
        return "[RL][" + id + "-" + status + "] ";
    }

    public int getId() {
        return id;
    }

    public String getClusterType() {
        return clusterType;
    }

    public String getStatus() {
        return status;
    }

    public String getTriggerType() {
        return triggerType;
    }

    public long getLastReloadStart() {
        return lastReloadStart;
    }

    public long getLastReloadEnd() {
        return lastReloadEnd;
    }

    public ConfStatus.Status getReloadType() {
        return confStatus.getStatus();
    }
}
