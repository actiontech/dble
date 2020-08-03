package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by szf on 2019/10/29.
 */
public class HaInfo {
    public enum HaStatus {
        INIT, SUCCESS, FAILED
    }

    public enum HaStartType {
        LOCAL_COMMAND, CLUSTER_NOTIFY
    }

    public enum HaStage {
        LOCAL_CHANGE, WAITING_FOR_OTHERS, RESPONSE_NOTIFY
    }

    public enum HaType {
        DATAHOST_DISABLE, DATAHOST_ENABLE, DATAHOST_SWITCH
    }


    private final HaType lockType;
    private final String startId;
    private final String dbGroupName;

    private final HaStatus status;
    private String split = ";";

    public HaInfo(String dbGroupName, String startId, HaType lockType, HaStatus status) {
        this.lockType = lockType;
        this.startId = startId;
        this.dbGroupName = dbGroupName;
        this.status = status;
    }

    public HaInfo(String kv) {
        String[] infoDetail = kv.split(split);
        this.lockType = HaType.valueOf(infoDetail[0]);
        this.startId = infoDetail[1];
        this.dbGroupName = infoDetail[2];
        this.status = HaStatus.valueOf(infoDetail[3]);
    }

    public HaType getLockType() {
        return lockType;
    }

    public String getStartId() {
        return startId;
    }

    public String getDbGroupName() {
        return dbGroupName;
    }

    public HaStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return lockType + split + startId + split + dbGroupName + split + status;
    }
}
