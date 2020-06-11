/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "shardingUser")
@XmlRootElement
public class ShardingUser extends User {
    @XmlAttribute(required = true)
    protected String schemas;
    @XmlAttribute
    protected String tenant;
    @XmlAttribute
    protected Boolean readOnly;
    @XmlAttribute
    protected String blacklist;

    protected Privileges privileges;


    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public Boolean getReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public String getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(String blacklist) {
        this.blacklist = blacklist;
    }

    public Privileges getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Privileges privileges) {
        this.privileges = privileges;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ShardingUser{").append(super.toString());
        sb.append(", schemas=").append(schemas);
        sb.append(", tenant=").append(tenant);
        sb.append(", readOnly=").append(readOnly);
        sb.append(", blacklist=").append(blacklist);

        if (privileges != null) {
            sb.append(", privileges=").append(privileges);
        }
        sb.append('}');
        return sb.toString();
    }

}
