/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;


import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.DbGroups;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbXmlToZkLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbXmlToZkLoader.class);
    private final String currZkPath;
    private static final String DB_XML_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "db.xml";
    private final Gson gson = new Gson();
    private XmlProcessBase xmlParseBase;

    public DbXmlToZkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                           XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = ClusterPathUtil.getDbConfPath();
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(DbGroups.class);
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        LOGGER.info("notifyProcess db.xml to zk ");
        String json = ClusterHelper.parseDbGroupXmlFileToJson(xmlParseBase, gson, DB_XML_PATH);
        this.checkAndWriteString(currZkPath, json);
        LOGGER.info("notifyProcess xml to zk is success");
        return true;
    }
}
