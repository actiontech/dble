/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.server;

import com.actiontech.dble.config.loader.zkprocess.entity.server.firewall.BlackList;
import com.actiontech.dble.config.loader.zkprocess.entity.server.firewall.WhiteHost;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by huqing.yan on 2017/6/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "firewall")
public class FireWall {
    @XmlElement
    protected WhiteHost whitehost;
    @XmlElement
    protected BlackList blacklist;

    public WhiteHost getWhitehost() {
        return whitehost;
    }

    public void setWhiteHost(WhiteHost whiteHost) {
        this.whitehost = whiteHost;
    }

    public BlackList getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(BlackList blacklist) {
        this.blacklist = blacklist;
    }

    @Override
    public String toString() {
        return "FireWall{whiteHost =" + whitehost + ", blacklist=" + blacklist + "}";
    }
}
