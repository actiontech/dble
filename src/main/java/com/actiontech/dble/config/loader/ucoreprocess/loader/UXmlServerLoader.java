/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
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
public class UXmlServerLoader implements UcoreXmlLoader {

    private ParseXmlServiceInf<Server> parseServerXMl;

    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    private static final String WRITEPATH = "server.xml";

    private static final String CONFIG_PATH = UcorePathUtil.getConfServerPath();

    private static final Logger LOGGER = LoggerFactory.getLogger(UXmlServerLoader.class);

    public UXmlServerLoader(XmlProcessBase xmlParseBase, UcoreClearKeyListener confListener) {
        this.parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }


    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Server server = new Server();
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        if (jsonObj.get(UcorePathUtil.FIREWALL) != null) {
            server.setFirewall(parseJsonFireWall.parseJsonToBean(jsonObj.getJSONObject(UcorePathUtil.FIREWALL).toJSONString()));
        }
        server.setVersion(jsonObj.getString(UcorePathUtil.VERSION));
        server.setSystem(parseJsonSystem.parseJsonToBean(jsonObj.getJSONObject(UcorePathUtil.DEFAULT).toJSONString()));
        server.setUser(parseJsonUser.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.USER).toJSONString()));
        String path = ResourceUtil.getResourcePathFromRoot(UcorePathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;
        this.parseServerXMl.parseToXmlWrite(server, path, "server");
    }

    @Override
    public void notifyCluster() throws Exception {
        Server servers = this.parseServerXMl.parseXmlToBean(UcorePathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject server = new JSONObject();
        server.put(UcorePathUtil.VERSION, servers.getVersion());
        server.put(UcorePathUtil.DEFAULT, servers.getSystem());
        server.put(UcorePathUtil.FIREWALL, servers.getFirewall());
        server.put(UcorePathUtil.USER, servers.getUser());
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, server.toJSONString());
    }

}
