/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingXmlToZKLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingXmlToZKLoader.class);
    private static final String SHARDING_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "sharding.xml";
    private final String currZkPath;
    private final Gson gson;
    private XmlProcessBase xmlParseBase;
    public ShardingXmlToZKLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                 XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = ClusterPathUtil.getConfShardingPath();
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Shardings.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        LOGGER.info("notifyProcess sharding.xml to zk ");
        String json = ClusterHelper.parseShardingXmlFileToJson(xmlParseBase, gson, SHARDING_PATH);
        this.checkAndWriteString(currZkPath, json);
        LOGGER.info("notifyProcess xml to zk is success");

        return true;
    }

}
