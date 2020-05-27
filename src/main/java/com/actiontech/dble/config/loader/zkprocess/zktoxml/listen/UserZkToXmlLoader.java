/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.Users;
import com.actiontech.dble.config.loader.zkprocess.entity.user.User;
import com.actiontech.dble.config.loader.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class UserZkToXmlLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserZkToXmlLoader.class);
    private final String currZkPath;
    private static final String USER_XML_PATH = "user.xml";
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public UserZkToXmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                             XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = ClusterPathUtil.getUserConfPath();
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Users.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        String jsonContent = this.getDataToString(currZkPath);
        LOGGER.info("notifyProcess zk to object users:" + jsonContent);
        Users userBean = ClusterHelper.parseUserJsonToBean(gson, jsonContent);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + USER_XML_PATH;

        xmlParseBase.baseParseAndWriteToXml(userBean, path, "user");
        LOGGER.info("notifyProcess zk to object writePath :" + path);


        return true;
    }


}
