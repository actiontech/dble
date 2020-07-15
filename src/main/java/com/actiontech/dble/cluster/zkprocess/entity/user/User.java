/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

@XmlTransient
@XmlSeeAlso({ShardingUser.class, ManagerUser.class, RwSplitUser.class})
public class User {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String password;
    @XmlAttribute
    protected String usingDecrypt;
    @XmlAttribute
    protected String whiteIPs;
    @XmlAttribute
    protected Integer maxCon;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsingDecrypt() {
        return usingDecrypt;
    }

    public void setUsingDecrypt(String usingDecrypt) {
        this.usingDecrypt = usingDecrypt;
    }

    public String getWhiteIPs() {
        return whiteIPs;
    }

    public void setWhiteIPs(String whiteIPs) {
        this.whiteIPs = whiteIPs;
    }


    public Integer getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(Integer maxCon) {
        this.maxCon = maxCon;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name=").append(name);
        sb.append(", password=").append(password);
        sb.append(", usingDecrypt=").append(usingDecrypt);
        sb.append(", maxCon=").append(maxCon);
        sb.append(", whiteIPs=").append(whiteIPs);
        return sb.toString();
    }
}
