/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

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
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DiretoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * ServerzkToxmlLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class ServerzkToxmlLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(ServerzkToxmlLoader.class);

    private final String currZkPath;

    private static final String WRITEPATH = "server.xml";

    private ParseXmlServiceInf<Server> parseServerXMl;

    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    public ServerzkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                               XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfServerPath();
        zookeeperListen.addToInit(this);
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DiretoryInf serverDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.SERVER, serverDirectory);

        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) serverDirectory.getSubordinateInfo().get(0);
        Server server = this.zktoServerBean(zkDirectory);

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object  zk server Object  :" + server);

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object writePath :" + path);

        this.parseServerXMl.parseToXmlWrite(server, path, "server");

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object zk server      write :" + path + " is success");

        return true;
    }

    /**
     * zktoServerBean
     *
     * @param zkDirectory
     * @return
     * @Created 2016/9/17
     * @create
     */
    private Server zktoServerBean(DiretoryInf zkDirectory) {
        Server server = new Server();

        DataInf serverZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DEFAULT);
        System systemValue = parseJsonSystem.parseJsonToBean(serverZkDirectory.getDataValue());
        server.setSystem(systemValue);

        DataInf firewallZkDirectory = this.getZkData(zkDirectory, KVPathUtil.FIREWALL);
        FireWall fireWall = parseJsonFireWall.parseJsonToBean(firewallZkDirectory.getDataValue());
        server.setFirewall(fireWall);

        DataInf userZkDirectory = this.getZkData(zkDirectory, KVPathUtil.USER);
        List<User> userList = parseJsonUser.parseJsonToBean(userZkDirectory.getDataValue());
        server.setUser(userList);

        return server;
    }


}
