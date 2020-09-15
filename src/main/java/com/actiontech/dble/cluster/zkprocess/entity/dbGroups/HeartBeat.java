/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.dbGroups;

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

    public HeartBeat() {
    }

    public HeartBeat(String value, Integer timeout, Integer errorRetryCount) {
        this.value = value;
        this.timeout = timeout;
        this.errorRetryCount = errorRetryCount;
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

    @Override
    public String toString() {
        return "heartbeat [timeout=" +
                timeout +
                ", errorRetryCount=" +
                errorRetryCount +
                ", value=" +
                value +
                "]";
    }

}
