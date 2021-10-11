/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public class OnlineType {
    private int serverPort;
    private String hostAddr;
    private long startTime;

    public OnlineType() {
    }

    public int getServerPort() {
        return serverPort;
    }

    public OnlineType setServerPort(int serverPortTmp) {
        this.serverPort = serverPortTmp;
        return this;
    }

    public String getHostAddr() {
        return hostAddr;
    }

    public OnlineType setHostAddr(String hostAddrTmp) {
        this.hostAddr = hostAddrTmp;
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public OnlineType setStartTime(long startTimeTmp) {
        this.startTime = startTimeTmp;
        return this;
    }

    @Override
    public String toString() {
        return "OnlineType{" +
                "serverPort=" + serverPort +
                ", hostAddr='" + hostAddr + '\'' +
                ", startTime=" + startTime +
                '}';
    }
}
