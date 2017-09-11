/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;

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
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SchemasxmlTozkLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SchemasxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SchemasxmlTozkLoader.class);

    private final String currZkPath;

    /**
     * schema path
     */
    private static final String SCHEMA_PATH = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + "schema.xml";

    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();

    public SchemasxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfSchemaPath();
        zookeeperListen.addToInit(this);
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        Schemas schema = this.parseSchemaXmlService.parseXmlToBean(SCHEMA_PATH);

        LOGGER.info("SchemasxmlTozkLoader notifyProcess xml to zk schema Object  :" + schema);

        this.xmlTozkSchemasJson(currZkPath, schema);

        LOGGER.info("SchemasxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * xmlTozkSchemasJson
     *
     * @param basePath
     * @param schema
     * @throws Exception
     * @Created 2016/9/17
     */
    private void xmlTozkSchemasJson(String basePath, Schemas schema) throws Exception {

        String schemaValueStr = this.parseJsonSchema.parseBeanToJson(schema.getSchema());

        this.checkAndwriteString(basePath, KVPathUtil.SCHEMA_SCHEMA, schemaValueStr);

        String dataNodeValueStr = this.parseJsonDataNode.parseBeanToJson(schema.getDataNode());

        this.checkAndwriteString(basePath, KVPathUtil.DATA_NODE, dataNodeValueStr);

        String dataHostValueStr = this.parseJsonDataHost.parseBeanToJson(schema.getDataHost());

        this.checkAndwriteString(basePath, KVPathUtil.DATA_HOST, dataHostValueStr);

    }

}
