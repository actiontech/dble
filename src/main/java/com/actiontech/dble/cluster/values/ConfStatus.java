/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

/**
 * Created by huqing.yan on 2017/7/10.
 */
public class ConfStatus {
    public enum Status {
        RELOAD_ALL,
        RELOAD_META
    }

    private String split = ";";
    private String from;
    private String params;
    private Status status;

    public ConfStatus(String from, Status statusFlag, String params) {
        this.from = from;
        this.status = statusFlag;
        this.params = params;
    }

    public ConfStatus(String info) {
        String[] infoDetail = info.split(split);
        this.from = infoDetail[0];
        this.status = Status.valueOf(infoDetail[1]);
        if (infoDetail.length == 3)
            this.params = infoDetail[2];
        else
            this.params = null;
    }

    @Override
    public String toString() {
        StringBuilder ss = new StringBuilder(from);
        ss.append(split);
        ss.append(status.toString());
        if (params != null) {
            ss.append(split);
            ss.append(params);
        }

        return ss.toString();
    }


    public String getFrom() {
        return from;
    }

    public Status getStatus() {
        return status;
    }

    public String getParams() {
        return params;
    }
}
