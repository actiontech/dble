/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.btrace.provider;

public class CostTimeProvider {
    public void beginRequest(long id) {
    }

    public void startProcess(long id) {
    }

    public void endParse(long id) {
    }

    public void endRoute(long id) {
    }

    public void resFromBack(long id) {
    }

    public void resLastBack(long id, long index) {

    }

    public void execLastBack(long id, long index) {

    }

    public void startExecuteBackend(long id) {
    }

    public void allBackendConnReceive(long id) {
    }

    public void beginResponse(long id) {
    }

    public void readyToDeliver(long id) {
    }
}
