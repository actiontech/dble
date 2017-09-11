/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by huqing.yan on 2017/7/10.
 */
public class ConfStatus {
    public enum Status {
        RELOAD,
        RELOAD_ALL,
        ROLLBACK
    }

    private String split = ";";
    private String from;
    private Status status;

    public ConfStatus(String from, Status statusFlag) {
        this.from = from;
        this.status = statusFlag;
    }

    public ConfStatus(String info) {
        String[] infos = info.split(split);
        this.from = infos[0];
        this.status = Status.valueOf(infos[1]);
    }

    @Override
    public String toString() {
        return from + split + status.toString();
    }


    public String getFrom() {
        return from;
    }

    public Status getStatus() {
        return status;
    }
}
