/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "rwSplitUser")
@XmlRootElement
public class RwSplitUser extends User {

    @XmlAttribute(required = true)
    protected String dbGroup;
    @XmlAttribute
    protected String tenant;
    @XmlAttribute
    protected String blacklist;

    public String getDbGroup() {
        return dbGroup;
    }

    public void setDbGroup(String dbGroup) {
        this.dbGroup = dbGroup;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(String blacklist) {
        this.blacklist = blacklist;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("rwSplitUser{").append(super.toString());
        sb.append(", dbGroup=").append(dbGroup);
        sb.append(", tenant=").append(tenant);
        sb.append(", blacklist=").append(blacklist);

        sb.append('}');
        return sb.toString();
    }

}
