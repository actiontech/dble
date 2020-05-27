/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlShardingLoader implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlShardingLoader.class);
    private static final String SHARDING_XML_PATH = "sharding.xml";
    private static final String CONFIG_PATH = ClusterPathUtil.getConfShardingPath();
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public XmlShardingLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Shardings.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        gson = gsonBuilder.create();
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (SystemConfig.getInstance().getInstanceName().equals(lock.getValue())) {
            return;
        }

        //the config Value in ucore is an all in one json config of the sharding.xml
        Shardings sharding = ClusterHelper.parseShardingJsonToBean(gson, configValue.getValue());
        ClusterHelper.writeMapFileAddFunction(sharding.getFunction());

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + SHARDING_XML_PATH;

        LOGGER.info("notifyProcess ucore to object writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(sharding, path, "sharding");

        LOGGER.info("notifyProcess ucore to object zk sharding write :" + path + " is success");

    }

    @Override
    public void notifyCluster() throws Exception {
        String path = ClusterPathUtil.LOCAL_WRITE_PATH + SHARDING_XML_PATH;
        String json = ClusterHelper.parseShardingXmlFileToJson(xmlParseBase, gson, path);
        ClusterHelper.setKV(CONFIG_PATH, json);
    }


}
