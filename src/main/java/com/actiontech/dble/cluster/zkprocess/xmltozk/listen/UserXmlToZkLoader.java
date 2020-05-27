/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;


import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserXmlToZkLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserXmlToZkLoader.class);
    private final String currZkPath;
    private static final String USER_XML_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "user.xml";
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public UserXmlToZkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                             XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = ClusterPathUtil.getUserConfPath();
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Users.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        LOGGER.info("notifyProcess user.xml to zk ");
        String json = ClusterHelper.parseUserXmlFileToJson(xmlParseBase, gson, USER_XML_PATH);
        this.checkAndWriteString(currZkPath, json);
        LOGGER.info("notifyProcess user.xml to zk is success");
        return true;
    }
}
