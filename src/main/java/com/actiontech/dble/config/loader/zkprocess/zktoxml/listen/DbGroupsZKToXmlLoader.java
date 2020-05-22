/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.DbGroups;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * SchemaszkToxmlLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class DbGroupsZKToXmlLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupsZKToXmlLoader.class);

    private final String currZkPath;
    private static final String DB_XML_PATH = "db.xml";
    private final Gson gson = new Gson();
    private XmlProcessBase xmlParseBase;

    public DbGroupsZKToXmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                 XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfShardingPath();
        zookeeperListen.addToInit(this);
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(DbGroups.class);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        String jsonContent = this.getDataToString(currZkPath);
        LOGGER.info("notifyProcess zk to object dbs:" + jsonContent);
        DbGroups dbGroups = ClusterHelper.parseDbGroupsJsonToBean(gson, jsonContent);

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator + DB_XML_PATH;
        LOGGER.info("notifyProcess zk to xml write Path :" + path);
        xmlParseBase.baseParseAndWriteToXml(dbGroups, path, "db");
        LOGGER.info("notifyProcess zk to  write :" + path + " is success");

        return true;
    }


}
