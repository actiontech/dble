/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.sharding.schema;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "globalTable")
@XmlRootElement
public class GlobalTable extends Table {

    @XmlAttribute
    private String checkClass;
    @XmlAttribute
    private String cron;

    public String getCheckClass() {
        return checkClass;
    }

    public void setCheckClass(String checkClass) {
        this.checkClass = checkClass;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    @Override
    public String toString() {
        return "globalTable [" + super.toString() +
                ", checkClass=" +
                checkClass +
                ", cron=" +
                cron +
                "]";
    }

}
