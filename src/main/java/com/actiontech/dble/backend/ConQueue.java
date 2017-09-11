/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConQueue {
    private final ConcurrentLinkedQueue<BackendConnection> autoCommitCons = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<BackendConnection> manCommitCons = new ConcurrentLinkedQueue<>();
    private long executeCount;

    public BackendConnection takeIdleCon(boolean autoCommit) {
        ConcurrentLinkedQueue<BackendConnection> f1 = autoCommitCons;
        ConcurrentLinkedQueue<BackendConnection> f2 = manCommitCons;

        if (!autoCommit) {
            f1 = manCommitCons;
            f2 = autoCommitCons;

        }
        BackendConnection con = f1.poll();
        if (con == null || con.isClosedOrQuit()) {
            con = f2.poll();
        }
        if (con == null || con.isClosedOrQuit()) {
            return null;
        } else {
            return con;
        }

    }

    public long getExecuteCount() {
        return executeCount;
    }

    public void incExecuteCount() {
        this.executeCount++;
    }

    public void removeCon(BackendConnection con) {
        if (!autoCommitCons.remove(con)) {
            manCommitCons.remove(con);
        }
    }

    public ConcurrentLinkedQueue<BackendConnection> getAutoCommitCons() {
        return autoCommitCons;
    }

    public ConcurrentLinkedQueue<BackendConnection> getManCommitCons() {
        return manCommitCons;
    }

    public ArrayList<BackendConnection> getIdleConsToClose(int count) {
        ArrayList<BackendConnection> readyCloseCons = new ArrayList<>(count);

        while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
            BackendConnection theCon = manCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }
        }

        while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
            BackendConnection theCon = autoCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }
        }

        return readyCloseCons;
    }

    public ArrayList<BackendConnection> getIdleConsToClose() {
        ArrayList<BackendConnection> readyCloseCons = new ArrayList<>(
                autoCommitCons.size() + manCommitCons.size());
        while (!manCommitCons.isEmpty()) {
            BackendConnection theCon = manCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }
        }

        while (!autoCommitCons.isEmpty()) {
            BackendConnection theCon = autoCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }
        }

        return readyCloseCons;
    }


}
