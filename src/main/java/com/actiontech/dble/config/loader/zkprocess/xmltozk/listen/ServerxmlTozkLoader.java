/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;

import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.Server;
import com.actiontech.dble.config.loader.zkprocess.entity.server.FireWall;
import com.actiontech.dble.config.loader.zkprocess.entity.server.System;
import com.actiontech.dble.config.loader.zkprocess.entity.server.User;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json.FireWallJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json.SystemJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json.UserJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.xml.ServerParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ServerxmlTozkLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class ServerxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(ServerxmlTozkLoader.class);

    private final String currZkPath;

    private static final String SERVER_PATH = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + "server.xml";


    private ParseXmlServiceInf<Server> parseServerXMl;

    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    public ServerxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                               XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfServerPath();
        zookeeperListen.addToInit(this);
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        Server server = this.parseServerXMl.parseXmlToBean(SERVER_PATH);
        LOGGER.info("ServerxmlTozkLoader notifyProcessxml to zk server Object  :" + server);
        this.xmlTozkServerJson(currZkPath, server);

        LOGGER.info("ServerxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * xmlTozkServerJson
     *
     * @param basePath
     * @param server
     * @throws Exception
     * @Created 2016/9/17
     */
    private void xmlTozkServerJson(String basePath, Server server) throws Exception {
        String defaultSystemValue = this.parseJsonSystem.parseBeanToJson(server.getSystem());
        this.checkAndwriteString(basePath, KVPathUtil.DEFAULT, defaultSystemValue);

        String firewallValueStr = this.parseJsonFireWall.parseBeanToJson(server.getFirewall());
        if (firewallValueStr == null) {
            firewallValueStr = "{}";
        }
        this.checkAndwriteString(basePath, KVPathUtil.FIREWALL, firewallValueStr);
        String userValueStr = this.parseJsonUser.parseBeanToJson(server.getUser());
        this.checkAndwriteString(basePath, KVPathUtil.USER, userValueStr);


    }
}
