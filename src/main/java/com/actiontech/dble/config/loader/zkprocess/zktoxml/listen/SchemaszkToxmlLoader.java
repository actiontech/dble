/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datanode.DataNode;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.schema.Schema;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.DataHostJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.DataNodeJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.SchemaJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DiretoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * SchemaszkToxmlLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SchemaszkToxmlLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaszkToxmlLoader.class);

    private final String currZkPath;

    private static final String WRITEPATH = "schema.xml";

    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();


    public SchemaszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfSchemaPath();
        zookeeperListen.addToInit(this);
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DiretoryInf schemaDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.SCHEMA, schemaDirectory);

        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) schemaDirectory.getSubordinateInfo().get(0);

        Schemas schema = this.zktoSchemasBean(zkDirectory);

        LOGGER.info("SchemasLoader notifyProcess zk to object  zk schema Object  :" + schema);

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess zk to object writePath :" + path);

        this.parseSchemaXmlService.parseToXmlWrite(schema, path, "schema");

        LOGGER.info("SchemasLoader notifyProcess zk to object zk schema      write :" + path + " is success");

        return true;
    }

    private Schemas zktoSchemasBean(ZkDirectoryImpl zkDirectory) {
        Schemas schema = new Schemas();
        DataInf schemaZkDirectory = this.getZkData(zkDirectory, KVPathUtil.SCHEMA_SCHEMA);
        List<Schema> schemaList = parseJsonSchema.parseJsonToBean(schemaZkDirectory.getDataValue());
        schema.setSchema(schemaList);

        DataInf dataNodeZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DATA_NODE);
        List<DataNode> dataNodeList = parseJsonDataNode.parseJsonToBean(dataNodeZkDirectory.getDataValue());
        schema.setDataNode(dataNodeList);

        DataInf dataHostZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DATA_HOST);
        List<DataHost> dataHostList = parseJsonDataHost.parseJsonToBean(dataHostZkDirectory.getDataValue());
        schema.setDataHost(dataHostList);


        return schema;

    }

}
