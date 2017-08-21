package io.mycat.config.loader.zkprocess.entity.server;

import io.mycat.config.loader.zkprocess.entity.server.firewall.BlackList;
import io.mycat.config.loader.zkprocess.entity.server.firewall.WhiteHost;

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
