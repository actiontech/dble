/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.server.firewall;

import com.actiontech.dble.config.loader.zkprocess.entity.server.firewall.whitehost.Host;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huqing.yan on 2017/6/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "whitehost")
public class WhiteHost {
    protected List<Host> host;

    public List<Host> getHost() {
        if (this.host == null) {
            host = new ArrayList<>();
        }
        return host;
    }

    public void setHost(List<Host> host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "WhiteHost{" + "host=" + host + '}';
    }
}
