package com.actiontech.dble.backend.datasource;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by szf on 2018/7/19.
 */
public class PhysicalDBPoolDiff {

    public static final String CHANGE_TYPE_ADD = "ADD";
    public static final String CHANGE_TYPE_DELETE = "DELETE";
    public static final String CHANGE_TYPE_CHANGE = "CHANGE";
    public static final String CHANGE_TYPE_NO = "NO_CHANGE";

    private String changeType = null;

    private PhysicalDBPool orgPool = null;

    private PhysicalDBPool newPool = null;


    private Set<BaseInfoDiff> baseDiff = null;

    private Set<PhysicalDatasourceDiff> hostChangeSet = null;


    public PhysicalDBPoolDiff(String changeType, PhysicalDBPool newPool, PhysicalDBPool orgPool) {
        this.changeType = changeType;
        this.newPool = newPool;
        this.orgPool = orgPool;
    }


    public PhysicalDBPoolDiff(PhysicalDBPool newPool, PhysicalDBPool orgPool) {
        this.orgPool = orgPool;
        this.newPool = newPool;
        if (!newPool.equalsBaseInfo(orgPool)) {
            this.changeType = CHANGE_TYPE_CHANGE;
            this.baseDiff = createBaseDiff(newPool, orgPool);
        }

        hostChangeSet = createHostChangeSet(newPool, orgPool);
        for (PhysicalDatasourceDiff diff : hostChangeSet) {
            if (!diff.getWriteHostChangeType().equals(CHANGE_TYPE_NO)) {
                this.changeType = CHANGE_TYPE_CHANGE;
                break;
            }
        }

        if (this.changeType == null) {
            this.changeType = CHANGE_TYPE_NO;
        }
    }


    private Set<PhysicalDatasourceDiff> createHostChangeSet(PhysicalDBPool newDbPool, PhysicalDBPool orgDbPool) {
        Set<PhysicalDatasourceDiff> hostDiff = new HashSet<PhysicalDatasourceDiff>();

        //add or not change
        for (int i = 0; i < newDbPool.getWriteSources().length; i++) {
            PhysicalDatasource writeHost = newDbPool.getWriteSources()[i];
            PhysicalDatasource[] readHost = newDbPool.getReadSources().get(new Integer(i));

            PhysicalDatasource orgHost = null;
            PhysicalDatasource[] relatedHost = null;
            for (int j = 0; j < orgDbPool.getWriteSources().length; j++) {
                PhysicalDatasource oldHost = orgDbPool.getWriteSources()[i];
                PhysicalDatasource[] oldRHost = orgDbPool.getReadSources().get(new Integer(i));
                if (oldHost.equals(writeHost) &&
                        ((oldRHost == null && readHost == null) || oldRHost.length == readHost.length)) {
                    boolean sameFlag = true;
                    if (oldRHost != null) {
                        for (int k = 0; k < oldRHost.length; k++) {
                            if (!oldRHost[k].equals(readHost[k])) {
                                sameFlag = false;
                                break;
                            }
                        }
                    }
                    if (sameFlag) {
                        orgHost = oldHost;
                        relatedHost = oldRHost;
                        break;
                    }
                } else {
                    continue;
                }
            }

            if (orgHost != null) {
                //can find a orgHost ,mings their is node all the same
                hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_NO, orgHost, relatedHost));
            } else {
                hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_ADD, writeHost, readHost));
            }
        }

        //add delete info into hostDiff & from hostDiff
        for (int i = 0; i < orgDbPool.getWriteSources().length; i++) {
            PhysicalDatasource writeHost = newDbPool.getWriteSources()[i];
            PhysicalDatasource[] readHost = newDbPool.getReadSources().get(new Integer(i));
            boolean findFlag = false;
            for (PhysicalDatasourceDiff diff : hostDiff) {
                if (diff.getSelfHost().equals(writeHost) && diff.getWriteHostChangeType().equals(CHANGE_TYPE_NO)) {
                    findFlag = true;
                    break;
                }
            }
            if (!findFlag) {
                hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_DELETE, writeHost, readHost));
            }
        }

        return hostDiff;
    }

    private Set<BaseInfoDiff> createBaseDiff(PhysicalDBPool newDbPool, PhysicalDBPool orgDbPool) {
        Set<BaseInfoDiff> baseDiffSet = new HashSet<BaseInfoDiff>();
        if (newDbPool.getDataHostConfig().getBalance() != orgDbPool.getDataHostConfig().getBalance()) {
            baseDiffSet.add(new BaseInfoDiff("balance", newDbPool.getDataHostConfig().getBalance(), orgDbPool.getDataHostConfig().getBalance()));
        }

        if (newDbPool.getDataHostConfig().getSwitchType() != orgDbPool.getDataHostConfig().getSwitchType()) {
            baseDiffSet.add(new BaseInfoDiff("switchType", newDbPool.getDataHostConfig().getSwitchType(), orgDbPool.getDataHostConfig().getSwitchType()));
        }

        if (newDbPool.getDataHostConfig().getMaxCon() != orgDbPool.getDataHostConfig().getMaxCon()) {
            baseDiffSet.add(new BaseInfoDiff("maxCon", newDbPool.getDataHostConfig().getMaxCon(), orgDbPool.getDataHostConfig().getMaxCon()));
        }

        if (newDbPool.getDataHostConfig().getMinCon() != orgDbPool.getDataHostConfig().getMinCon()) {
            baseDiffSet.add(new BaseInfoDiff("minCon", newDbPool.getDataHostConfig().getMinCon(), orgDbPool.getDataHostConfig().getMinCon()));
        }


        if (newDbPool.getDataHostConfig().getSlaveThreshold() != orgDbPool.getDataHostConfig().getSlaveThreshold()) {
            baseDiffSet.add(new BaseInfoDiff("slaveThreshold", newDbPool.getDataHostConfig().getSlaveThreshold(), orgDbPool.getDataHostConfig().getSlaveThreshold()));
        }


        if (!newDbPool.getDataHostConfig().getHearbeatSQL().equals(orgDbPool.getDataHostConfig().getHearbeatSQL())) {
            baseDiffSet.add(new BaseInfoDiff("minCon", newDbPool.getDataHostConfig().getHearbeatSQL(), orgDbPool.getDataHostConfig().getHearbeatSQL()));
        }

        if (newDbPool.getDataHostConfig().isTempReadHostAvailable() != orgDbPool.getDataHostConfig().isTempReadHostAvailable()) {
            baseDiffSet.add(new BaseInfoDiff("slaveThreshold", newDbPool.getDataHostConfig().isTempReadHostAvailable() ? 1 : 0,
                    orgDbPool.getDataHostConfig().isTempReadHostAvailable() ? 1 : 0));
        }

        return baseDiffSet;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }

    public PhysicalDBPool getOrgPool() {
        return orgPool;
    }

    public void setOrgPool(PhysicalDBPool orgPool) {
        this.orgPool = orgPool;
    }

    public PhysicalDBPool getNewPool() {
        return newPool;
    }

    public void setNewPool(PhysicalDBPool newPool) {
        this.newPool = newPool;
    }

    public Set<BaseInfoDiff> getBaseDiff() {
        return baseDiff;
    }

    public void setBaseDiff(Set<BaseInfoDiff> baseDiff) {
        this.baseDiff = baseDiff;
    }

    public Set<PhysicalDatasourceDiff> getHostChangeSet() {
        return hostChangeSet;
    }

    public void setHostChangeSet(Set<PhysicalDatasourceDiff> hostChangeSet) {
        this.hostChangeSet = hostChangeSet;
    }

    private class PhysicalDatasourceDiff {
        private String writeHostChangeType = null;


        private PhysicalDatasource selfHost;

        private PhysicalDatasource[] relatedHost;

        PhysicalDatasourceDiff(String writeHostChangeType, PhysicalDatasource selfHost, PhysicalDatasource[] relatedHost) {
            this.writeHostChangeType = writeHostChangeType;
            this.selfHost = selfHost;
            this.relatedHost = relatedHost;
        }


        public String getWriteHostChangeType() {
            return writeHostChangeType;
        }

        public void setWriteHostChangeType(String writeHostChangeType) {
            this.writeHostChangeType = writeHostChangeType;
        }

        public PhysicalDatasource getSelfHost() {
            return selfHost;
        }

        public void setSelfHost(PhysicalDatasource selfHost) {
            this.selfHost = selfHost;
        }

        public PhysicalDatasource[] getRelatedHost() {
            return relatedHost;
        }

        public void setRelatedHost(PhysicalDatasource[] relatedHost) {
            this.relatedHost = relatedHost;
        }


    }


    private class BaseInfoDiff {
        String type;
        String orgValue;
        String newVaule;

        BaseInfoDiff(String type, int orgVaule, int newValue) {
            this.type = type;
            this.orgValue = orgVaule + "";
            this.newVaule = newValue + "";
        }


        BaseInfoDiff(String type, String orgVaule, String newValue) {
            this.type = type;
            this.orgValue = orgVaule;
            this.newVaule = newValue;
        }
    }
}
