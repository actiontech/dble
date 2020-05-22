/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.zkprocess.entity.user.BlackList;
import com.actiontech.dble.config.loader.zkprocess.entity.user.ManagerUser;
import com.actiontech.dble.config.loader.zkprocess.entity.user.RwSplitUser;
import com.actiontech.dble.config.loader.zkprocess.entity.user.ShardingUser;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "user")
public class Users {
    @XmlAttribute
    protected String version;
    @XmlElementRefs({
            @XmlElementRef(name = "ShardingUser", type = ShardingUser.class),
            @XmlElementRef(name = "ManagerUser", type = ManagerUser.class),
            @XmlElementRef(name = "RwSplitUser", type = RwSplitUser.class)
    })
    protected List<Object> user;

    private List<BlackList> blacklist;

    public List<Object> getUser() {
        return user;
    }

    public void setUser(List<Object> user) {
        this.user = user;
    }

    public List<BlackList> getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(List<BlackList> blacklist) {
        this.blacklist = blacklist;
    }


    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "users [user=" +
                user +
                ", blacklist=" +
                blacklist +
                "]";
    }

}
