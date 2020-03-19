/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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

    private AbstractPhysicalDBPool orgPool = null;

    private AbstractPhysicalDBPool newPool = null;


    //private Set<BaseInfoDiff> baseDiff = null;


    public PhysicalDBPoolDiff(AbstractPhysicalDBPool newPool, AbstractPhysicalDBPool orgPool) {
        this.orgPool = orgPool;
        this.newPool = newPool;
        if (!newPool.equalsBaseInfo(orgPool)) {
            this.changeType = CHANGE_TYPE_CHANGE;
            //this.baseDiff = createBaseDiff(newPool, orgPool);
        }

        Set<PhysicalDatasourceDiff> hostChangeSet = createHostChangeSet(newPool, orgPool);
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


    private Set<PhysicalDatasourceDiff> createHostChangeSet(AbstractPhysicalDBPool newDbPool, AbstractPhysicalDBPool orgDbPool) {
        Set<PhysicalDatasourceDiff> hostDiff = new HashSet<>();

        //add or not change
        PhysicalDatasource newWriteHost = newDbPool.getWriteSource();
        PhysicalDatasource[] newReadHost = newDbPool.getReadSources();

        PhysicalDatasource oldHost = orgDbPool.getWriteSource();
        PhysicalDatasource[] oldRHost = orgDbPool.getReadSources();

        boolean sameFlag = false;
        if (oldHost.equals(newWriteHost) &&
                ((oldRHost == null && newReadHost == null) || ((oldRHost != null && newReadHost != null) && oldRHost.length == newReadHost.length))) {
            //compare the newReadHost is the same
            sameFlag = calculateForDataSources(oldRHost, newReadHost);
        }

        if (sameFlag) {
            oldHost.setTestConnSuccess(newWriteHost.isTestConnSuccess());
            //can find a orgHost ,means their is node all the same
            hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_NO, oldHost, oldRHost));
        } else {
            hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_ADD, newWriteHost, newReadHost));
            hostDiff.add(new PhysicalDatasourceDiff(CHANGE_TYPE_DELETE, oldHost, oldRHost));
        }

        return hostDiff;
    }


    private boolean calculateForDataSources(PhysicalDatasource[] olds, PhysicalDatasource[] news) {
        if (olds != null) {
            for (int k = 0; k < olds.length; k++) {
                if (!olds[k].equals(news[k])) {
                    return false;
                } else {
                    olds[k].setTestConnSuccess(news[k].isTestConnSuccess());
                }
            }
        }
        return true;
    }


    public String getChangeType() {
        return changeType;
    }

    public AbstractPhysicalDBPool getOrgPool() {
        return orgPool;
    }

    public AbstractPhysicalDBPool getNewPool() {
        return newPool;
    }

}
