/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.zkprocess.entity.server.FireWall;
import com.actiontech.dble.config.loader.zkprocess.entity.server.System;
import com.actiontech.dble.config.loader.zkprocess.entity.server.User;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "server")
public class Server {

    @XmlAttribute(required = false)
    protected String version;

    @XmlElement(required = true)
    protected System system;
    @XmlElement
    protected FireWall firewall;
    @XmlElement(required = true)
    protected List<User> user;

    public System getSystem() {
        return system;
    }

    public void setSystem(System value) {
        this.system = value;
    }

    public List<User> getUser() {
        return user;
    }

    public void setUser(List<User> user) {
        this.user = user;
    }


    public FireWall getFirewall() {
        return firewall;
    }

    public void setFirewall(FireWall firewall) {
        this.firewall = firewall;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Server [system=");
        builder.append(system);
        builder.append(", user=");
        builder.append(user);
        if (firewall != null) {
            builder.append(", firewall=");
            builder.append(firewall);
        }
        builder.append("]");
        return builder.toString();
    }

}
