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
public class PhysicalDataHostDiff {

    public static final String CHANGE_TYPE_ADD = "ADD";
    public static final String CHANGE_TYPE_DELETE = "DELETE";
    public static final String CHANGE_TYPE_CHANGE = "CHANGE";
    public static final String CHANGE_TYPE_NO = "NO_CHANGE";

    private String changeType = null;

    private PhysicalDataHost orgPool = null;

    private PhysicalDataHost newPool = null;


    //private Set<BaseInfoDiff> baseDiff = null;


    public PhysicalDataHostDiff(PhysicalDataHost newPool, PhysicalDataHost orgPool) {
        this.orgPool = orgPool;
        this.newPool = newPool;
        if (!newPool.equalsBaseInfo(orgPool)) {
            this.changeType = CHANGE_TYPE_CHANGE;
            //this.baseDiff = createBaseDiff(newPool, orgPool);
        }

        Set<PhysicalDataSourceDiff> hostChangeSet = createHostChangeSet(newPool, orgPool);
        for (PhysicalDataSourceDiff diff : hostChangeSet) {
            if (!diff.getWriteHostChangeType().equals(CHANGE_TYPE_NO)) {
                this.changeType = CHANGE_TYPE_CHANGE;
                break;
            }
        }

        if (this.changeType == null) {
            this.changeType = CHANGE_TYPE_NO;
        }
    }


    private Set<PhysicalDataSourceDiff> createHostChangeSet(PhysicalDataHost newDataHost, PhysicalDataHost orgDataHost) {
        Set<PhysicalDataSourceDiff> hostDiff = new HashSet<>();

        //add or not change
        PhysicalDataSource newWriteHost = newDataHost.getWriteSource();
        PhysicalDataSource[] newReadHost = newDataHost.getReadSources();

        PhysicalDataSource oldHost = orgDataHost.getWriteSource();
        PhysicalDataSource[] oldRHost = orgDataHost.getReadSources();

        boolean sameFlag = false;
        if (oldHost.equals(newWriteHost) &&
                ((oldRHost == null && newReadHost == null) || ((oldRHost != null && newReadHost != null) && oldRHost.length == newReadHost.length))) {
            //compare the newReadHost is the same
            sameFlag = calculateForDataSources(oldRHost, newReadHost);
        }

        if (sameFlag) {
            oldHost.setTestConnSuccess(newWriteHost.isTestConnSuccess());
            //can find a orgHost ,means their is node all the same
            hostDiff.add(new PhysicalDataSourceDiff(CHANGE_TYPE_NO, oldHost, oldRHost));
        } else {
            hostDiff.add(new PhysicalDataSourceDiff(CHANGE_TYPE_ADD, newWriteHost, newReadHost));
            hostDiff.add(new PhysicalDataSourceDiff(CHANGE_TYPE_DELETE, oldHost, oldRHost));
        }

        return hostDiff;
    }


    private boolean calculateForDataSources(PhysicalDataSource[] olds, PhysicalDataSource[] news) {
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

    public PhysicalDataHost getOrgPool() {
        return orgPool;
    }

    public PhysicalDataHost getNewPool() {
        return newPool;
    }

}
