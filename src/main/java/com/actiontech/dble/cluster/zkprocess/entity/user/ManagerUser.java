/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "managerUser")
@XmlRootElement
public class ManagerUser extends User {
    @XmlAttribute
    protected Boolean readOnly;



    public Boolean getReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public String toString() {
        String sb = "managerUser{" + super.toString() +
                ", readOnly=" + readOnly +
                '}';
        return sb;
    }

}
