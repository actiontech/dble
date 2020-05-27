/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.entity.DbGroups;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlDbLoader implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlDbLoader.class);
    private static final String DB_XML_PATH = "db.xml";
    private static final String CONFIG_PATH = ClusterPathUtil.getDbConfPath();
    private final Gson gson = new Gson();
    private XmlProcessBase xmlParseBase;



    public XmlDbLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(DbGroups.class);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (SystemConfig.getInstance().getInstanceId().equals(lock.getValue())) {
            return;
        }
        DbGroups dbs = ClusterHelper.parseDbGroupsJsonToBean(gson, configValue.getValue());

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + DB_XML_PATH;

        LOGGER.info("notifyProcess ucore to xml write Path :" + path);

        this.xmlParseBase.baseParseAndWriteToXml(dbs, path, "db");

        LOGGER.info("notifyProcess ucore to xml write :" + path + " is success");

    }

    @Override
    public void notifyCluster() throws Exception {
        String path = ClusterPathUtil.LOCAL_WRITE_PATH + DB_XML_PATH;
        String json = ClusterHelper.parseDbGroupXmlFileToJson(xmlParseBase, gson, path);
        ClusterHelper.setKV(CONFIG_PATH, json);
    }

}
