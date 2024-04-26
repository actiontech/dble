/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

/**
 * Created by huqing.yan on 2017/7/10.
 */
public class ConfStatus {
    public enum Status {
        RELOAD_ALL,
        RELOAD_META,
        MANAGER_INSERT,
        MANAGER_UPDATE,
        MANAGER_DELETE
    }

    private String split = ";";
    private String from;
    private String params;
    private final Status status;
    private String extraInfo;

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
        else if (infoDetail.length == 4) {
            this.params = infoDetail[2];
            this.extraInfo = new String(Base64.decode(infoDetail[3]));
        } else {
            this.params = null;
            this.extraInfo = null;
        }

    }

    public ConfStatus(Status status, String extraInfo) {
        this.status = status;
        this.extraInfo = extraInfo;
    }

    public ConfStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuilder ss = new StringBuilder(from);
        ss.append(split);
        ss.append(status.toString());
        if (params != null) {
            ss.append(split);
            ss.append(params);
            if (extraInfo != null) {
                ss.append(split);
                ss.append(Base64.encode(extraInfo.getBytes()));
            }
        }

        return ss.toString();
    }

    public String getStatusAExtraInfo() {
        StringBuilder ss = new StringBuilder();
        ss.append(this.status);
        if (this.extraInfo != null) {
            ss.append(":" + this.extraInfo);
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

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }
}
