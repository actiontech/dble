/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
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
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlServerLoader implements ClusterXmlLoader {

    private ParseXmlServiceInf<Server> parseServerXMl;

    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    private static final String WRITEPATH = "server.xml";

    private static final String CONFIG_PATH = ClusterPathUtil.getConfServerPath();

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlServerLoader.class);

    public XmlServerLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }


    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Server server = new Server();
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        if (jsonObj.get(ClusterPathUtil.FIREWALL) != null) {
            server.setFirewall(parseJsonFireWall.parseJsonToBean(jsonObj.getJSONObject(ClusterPathUtil.FIREWALL).toJSONString()));
        }
        server.setVersion(jsonObj.getString(ClusterPathUtil.VERSION));
        server.setSystem(parseJsonSystem.parseJsonToBean(jsonObj.getJSONObject(ClusterPathUtil.DEFAULT).toJSONString()));
        server.setUser(parseJsonUser.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.USER).toJSONString()));
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;
        this.parseServerXMl.parseToXmlWrite(server, path, "server");
    }

    @Override
    public void notifyCluster() throws Exception {
        Server servers = this.parseServerXMl.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject server = new JSONObject();
        server.put(ClusterPathUtil.VERSION, servers.getVersion());
        server.put(ClusterPathUtil.DEFAULT, servers.getSystem());
        server.put(ClusterPathUtil.FIREWALL, servers.getFirewall());
        server.put(ClusterPathUtil.USER, servers.getUser());
        ClusterHelper.setKV(CONFIG_PATH, server.toJSONString());
    }

}
