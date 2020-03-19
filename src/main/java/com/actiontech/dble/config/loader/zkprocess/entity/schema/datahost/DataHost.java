/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

/**
 * <dataHost name="localhost1" maxCon="1000" minCon="10" balance="0"
 * writeType="0" dbType="mysql" dbDriver="native" switchType="1"  slaveThreshold="100">
 * </dataHost>
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dataHost")
public class DataHost implements Named {

    @XmlAttribute(required = true)
    protected Integer balance;
    @XmlAttribute(required = true)
    protected Integer maxCon;
    @XmlAttribute(required = true)
    protected Integer minCon;
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected Integer slaveThreshold;
    @XmlAttribute
    protected Integer tempReadHostAvailable;
    @XmlAttribute
    protected Integer keepOrig;

    protected HeartBeat heartbeat;

    protected WriteHost writeHost;
    protected ReadHost readHost;

    public HeartBeat getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(HeartBeat heartbeat) {
        this.heartbeat = heartbeat;
    }

    public WriteHost getWriteHost() {
        return writeHost;
    }

    public void setWriteHost(WriteHost writeHost) {
        this.writeHost = writeHost;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(Integer maxCon) {
        this.maxCon = maxCon;
    }

    public Integer getMinCon() {
        return minCon;
    }

    public void setMinCon(Integer minCon) {
        this.minCon = minCon;
    }

    public Integer getBalance() {
        return balance;
    }

    public void setBalance(Integer balance) {
        this.balance = balance;
    }


    public Integer getSlaveThreshold() {
        return slaveThreshold;
    }

    public void setSlaveThreshold(Integer slaveThreshold) {
        this.slaveThreshold = slaveThreshold;
    }


    public Integer getTempReadHostAvailable() {
        return tempReadHostAvailable;
    }

    public void setTempReadHostAvailable(Integer tempReadHostAvailable) {
        this.tempReadHostAvailable = tempReadHostAvailable;
    }

    public Integer getKeepOrig() {
        return keepOrig;
    }

    public void setKeepOrig(Integer keepOrig) {
        this.keepOrig = keepOrig;
    }


    public ReadHost getReadHost() {
        return readHost;
    }

    public void setReadHost(ReadHost readHost) {
        this.readHost = readHost;
    }


    @Override
    public String toString() {
        String builder = "DataHost [balance=" +
                balance +
                ", maxCon=" +
                maxCon +
                ", minCon=" +
                minCon +
                ", name=" +
                name +
                ", slaveThreshold=" +
                slaveThreshold +
                ", tempReadHostAvailable=" +
                tempReadHostAvailable +
                ", keepOrig=" +
                keepOrig +
                ", heartbeat=" +
                heartbeat.toString() +
                ", writeHost=[" +
                writeHost +
                "]";
        return builder;
    }

}
