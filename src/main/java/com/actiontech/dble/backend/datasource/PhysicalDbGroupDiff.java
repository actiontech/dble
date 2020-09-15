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
public class PhysicalDbGroupDiff {

    public static final String CHANGE_TYPE_ADD = "ADD";
    public static final String CHANGE_TYPE_DELETE = "DELETE";
    public static final String CHANGE_TYPE_CHANGE = "CHANGE";
    public static final String CHANGE_TYPE_NO = "NO_CHANGE";

    private String changeType = null;
    private PhysicalDbGroup orgPool;
    private PhysicalDbGroup newPool;

    public PhysicalDbGroupDiff(PhysicalDbGroup newPool, PhysicalDbGroup orgPool) {
        this.orgPool = orgPool;
        this.newPool = newPool;
        if (!newPool.equalsBaseInfo(orgPool)) {
            this.changeType = CHANGE_TYPE_CHANGE;
            //this.baseDiff = createBaseDiff(newPool, orgPool);
        }

        Set<PhysicalDbInstanceDiff> hostChangeSet = createHostChangeSet(newPool, orgPool);
        for (PhysicalDbInstanceDiff diff : hostChangeSet) {
            if (!diff.getWriteHostChangeType().equals(CHANGE_TYPE_NO)) {
                this.changeType = CHANGE_TYPE_CHANGE;
                break;
            }
        }

        if (this.changeType == null) {
            this.changeType = CHANGE_TYPE_NO;
        }
    }

    private Set<PhysicalDbInstanceDiff> createHostChangeSet(PhysicalDbGroup newDbGroup, PhysicalDbGroup orgDbGroup) {
        Set<PhysicalDbInstanceDiff> hostDiff = new HashSet<>();

        //add or not change
        PhysicalDbInstance newWriteHost = newDbGroup.getWriteDbInstance();
        PhysicalDbInstance[] newReadHost = newDbGroup.getReadDbInstances();

        PhysicalDbInstance oldHost = orgDbGroup.getWriteDbInstance();
        PhysicalDbInstance[] oldRHost = orgDbGroup.getReadDbInstances();

        boolean sameFlag = false;
        if (oldHost.equals(newWriteHost) && oldRHost.length == newReadHost.length) {
            //compare the newReadHost is the same
            sameFlag = calculateForDbInstances(oldRHost, newReadHost);
        }

        if (sameFlag) {
            oldHost.setTestConnSuccess(newWriteHost.isTestConnSuccess());
            //can find a orgHost ,means their is node all the same
            hostDiff.add(new PhysicalDbInstanceDiff(CHANGE_TYPE_NO, oldHost, oldRHost));
        } else {
            hostDiff.add(new PhysicalDbInstanceDiff(CHANGE_TYPE_ADD, newWriteHost, newReadHost));
            hostDiff.add(new PhysicalDbInstanceDiff(CHANGE_TYPE_DELETE, oldHost, oldRHost));
        }

        return hostDiff;
    }

    private boolean calculateForDbInstances(PhysicalDbInstance[] olds, PhysicalDbInstance[] news) {
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

    public PhysicalDbGroup getOrgPool() {
        return orgPool;
    }

    public PhysicalDbGroup getNewPool() {
        return newPool;
    }

}
