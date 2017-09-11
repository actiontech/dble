/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

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
    protected Integer switchType;
    @XmlAttribute
    protected Integer slaveThreshold;
    @XmlAttribute
    protected Integer tempReadHostAvailable;

    protected String heartbeat;

    protected List<WriteHost> writeHost;

    public String getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(String heartbeat) {
        this.heartbeat = heartbeat;
    }

    public List<WriteHost> getWriteHost() {
        if (this.writeHost == null) {
            writeHost = new ArrayList<>();
        }
        return writeHost;
    }

    public void setWriteHost(List<WriteHost> writeHost) {
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


    public Integer getSwitchType() {
        return switchType;
    }

    public void setSwitchType(Integer switchType) {
        this.switchType = switchType;
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
                ", switchType=" +
                switchType +
                ", slaveThreshold=" +
                slaveThreshold +
                ", tempReadHostAvailable=" +
                tempReadHostAvailable +
                ", heartbeat=" +
                heartbeat +
                ", writeHost=" +
                writeHost +
                "]";
        return builder;
    }

}
