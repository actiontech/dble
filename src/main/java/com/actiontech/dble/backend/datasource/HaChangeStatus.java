/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.cluster.values.HaInfo;

/**
 * Created by szf on 2019/11/1.
 */
public class HaChangeStatus {
    private final int index;
    private final String command;
    private final HaInfo.HaStartType type;
    private final long startTimeStamp;
    private volatile long endTimeStamp;
    private HaInfo.HaStage stage;

    public HaChangeStatus(int index, String command, HaInfo.HaStartType type, long startTimeStamp, HaInfo.HaStage stage) {
        this.index = index;
        this.command = command;
        this.type = type;
        this.startTimeStamp = startTimeStamp;
        this.stage = stage;
    }

    public int getIndex() {
        return index;
    }

    public String getCommand() {
        return command;
    }

    public HaInfo.HaStartType getType() {
        return type;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public long getEndTimeStamp() {
        return endTimeStamp;
    }

    public HaInfo.HaStage getStage() {
        return stage;
    }

    public void setEndTimeStamp(long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    public void setStage(HaInfo.HaStage stage) {
        this.stage = stage;
    }

}
