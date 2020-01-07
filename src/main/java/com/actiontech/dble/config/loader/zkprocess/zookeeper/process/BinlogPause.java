/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by huqing.yan on 2017/6/12.
 */
public class BinlogPause {
    public enum BinlogPauseStatus {
        ON, OFF
    }

    private String from;
    private BinlogPauseStatus status;
    private String split = ";";

    public BinlogPause(String from, BinlogPauseStatus pauseStatus) {
        this.from = from;
        this.status = pauseStatus;
    }

    public BinlogPause(String info) {
        String[] infoDetail = info.split(split);
        this.from = infoDetail[0];
        this.status = BinlogPauseStatus.valueOf(infoDetail[1]);
    }

    @Override
    public String toString() {
        return from + split + status.toString();
    }

    public String getFrom() {
        return from;
    }

    public BinlogPauseStatus getStatus() {
        return status;
    }
}
