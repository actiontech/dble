/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.Shardings;
import com.actiontech.dble.config.loader.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.config.loader.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ShardingZkToXmlLoader extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingZkToXmlLoader.class);
    private final String currZkPath;
    private static final String SHARDING_XML_PATH = "sharding.xml";
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public ShardingZkToXmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                 XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfShardingPath();
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Shardings.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        String jsonContent = this.getDataToString(currZkPath);
        LOGGER.info("notifyProcess zk to object shardings:" + jsonContent);
        Shardings shardingBean = ClusterHelper.parseShardingJsonToBean(gson, jsonContent);
        ClusterHelper.writeMapFileAddFunction(shardingBean.getFunction());

        LOGGER.info("notifyProcess write mapFile is success ");

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator + SHARDING_XML_PATH;

        LOGGER.info("notifyProcess zk to object writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(shardingBean, path, "sharding");

        LOGGER.info("notifyProcess zk to object shardings, write :" + path + " is success");

        return true;
    }


}
