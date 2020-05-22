/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.entity.Users;
import com.actiontech.dble.config.loader.zkprocess.entity.user.User;
import com.actiontech.dble.config.loader.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class XmlUserLoader implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlUserLoader.class);
    private static final String USER_XML_PATH = "user.xml";
    private static final String CONFIG_PATH = ClusterPathUtil.getConfUserPath();
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public XmlUserLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Users.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        gson = gsonBuilder.create();
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (SystemConfig.getInstance().getInstanceId().equals(lock.getValue())) {
            return;
        }

        //the config Value in ucore is an all in one json config of the user.xml
        Users users = ClusterHelper.parseUserJsonToBean(gson, configValue.getValue());

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + USER_XML_PATH;

        LOGGER.info("notifyProcess ucore to object writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(users, path, "user");

        LOGGER.info("notifyProcess ucore to object zk user write :" + path + " is success");

    }

    @Override
    public void notifyCluster() throws Exception {
        String path = ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + USER_XML_PATH;
        String json = ClusterHelper.parseUserXmlFileToJson(xmlParseBase, gson, path);
        ClusterHelper.setKV(CONFIG_PATH, json);
    }


}
