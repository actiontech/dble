/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups;


import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "heartbeat")
public class HeartBeat {
    @XmlValue
    protected String value;

    @XmlAttribute
    protected Integer timeout;

    @XmlAttribute
    protected Integer errorRetryCount;

    @XmlAttribute
    protected Integer keepAlive;

    public HeartBeat() {
    }

    public HeartBeat(String value, Integer timeout, Integer errorRetryCount, Integer keepAlive) {
        this.value = value;
        this.timeout = timeout;
        this.errorRetryCount = errorRetryCount;
        this.keepAlive = keepAlive;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getErrorRetryCount() {
        return errorRetryCount;
    }

    public void setErrorRetryCount(Integer errorRetryCount) {
        this.errorRetryCount = errorRetryCount;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Integer keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public String toString() {
        return "heartbeat [timeout=" +
                timeout +
                ", errorRetryCount=" +
                errorRetryCount +
                ", keepAlive=" +
                keepAlive +
                ", value=" +
                value +
                "]";
    }

}
