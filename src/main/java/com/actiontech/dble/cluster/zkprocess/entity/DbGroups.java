/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity;

import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.StringUtil;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "db")
public class DbGroups {

    @XmlAttribute(required = false)
    protected String version;

    protected List<DBGroup> dbGroup;

    public List<DBGroup> getDbGroup() {
        if (this.dbGroup == null) {
            dbGroup = new ArrayList<>();
        }
        return dbGroup;
    }

    public void setDbGroup(List<DBGroup> dbGroup) {
        this.dbGroup = dbGroup;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void addDbGroup(DBGroup group) {
        getDbGroup().add(group);
    }

    @Override
    public String toString() {
        return "dbGroups [" +
                dbGroup +
                "]";
    }

    public void encryptPassword() {
        for (DBGroup group : getDbGroup()) {
            for (DBInstance dbInstance : group.getDbInstance()) {
                String usingDecrypt = dbInstance.getUsingDecrypt();
                if (!StringUtil.isEmpty(usingDecrypt) && Boolean.parseBoolean(usingDecrypt)) {
                    dbInstance.setPassword(getPasswordEncrypt(dbInstance.getName(), dbInstance.getUser(), dbInstance.getPassword()));
                }
            }
        }
    }


    private static String getPasswordEncrypt(String instanceName, String name, String password) {
        try {
            return DecryptUtil.encrypt("1:" + instanceName + ":" + name + ":" + password);
        } catch (Exception e) {
            return "******";
        }
    }


}
