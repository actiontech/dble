/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.dbGroups;

import com.actiontech.dble.cluster.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dbGroup")
public class DBGroup implements Named {

    @XmlAttribute(required = true)
    protected Integer rwSplitMode;
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected Integer delayThreshold;
    @XmlAttribute
    protected String disableHA;

    protected HeartBeat heartbeat;
    protected List<DBInstance> dbInstance;

    public DBGroup() {
    }

    public DBGroup(Integer rwSplitMode, String name, Integer delayThreshold, String disableHA, HeartBeat heartbeat) {
        this.rwSplitMode = rwSplitMode;
        this.name = name;
        this.delayThreshold = delayThreshold;
        this.disableHA = disableHA;
        this.heartbeat = heartbeat;
    }

    public HeartBeat getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(HeartBeat heartbeat) {
        this.heartbeat = heartbeat;
    }


    public List<DBInstance> getDbInstance() {
        if (this.dbInstance == null) {
            dbInstance = new ArrayList<>();
        }
        return dbInstance;
    }

    public void addDbInstance(DBInstance instance) {
        getDbInstance().add(instance);
    }

    public void addAllDbInstance(List<DBInstance> instance) {
        getDbInstance().addAll(instance);
    }

    public void setDbInstance(List<DBInstance> dbInstance) {
        this.dbInstance = dbInstance;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getRwSplitMode() {
        return rwSplitMode;
    }

    public void setRwSplitMode(Integer rwSplitMode) {
        this.rwSplitMode = rwSplitMode;
    }


    public Integer getDelayThreshold() {
        return delayThreshold;
    }

    public void setDelayThreshold(Integer delayThreshold) {
        this.delayThreshold = delayThreshold;
    }

    public String getDisableHA() {
        return disableHA;
    }

    public void setDisableHA(String disableHA) {
        this.disableHA = disableHA;
    }


    @Override
    public String toString() {
        String builder = "dbGroup [rwSplitMode=" +
                rwSplitMode +
                ", name=" +
                name +
                ", delayThreshold=" +
                delayThreshold +
                ", disableHA=" +
                disableHA +
                ", heartbeat=" +
                heartbeat.toString() +
                ", dbInstances=[" +
                dbInstance +
                "]";
        return builder;
    }
}
