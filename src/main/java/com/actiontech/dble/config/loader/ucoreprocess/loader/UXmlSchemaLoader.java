/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
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
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public class UXmlSchemaLoader implements UcoreXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UXmlSchemaLoader.class);

    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();

    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    private static final String WRITEPATH = "schema.xml";

    private static final String CONFIG_PATH = UcorePathUtil.getConfSchemaPath();


    public UXmlSchemaLoader(XmlProcessBase xmlParseBase, UcoreClearKeyListener confListener) {
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Schemas schema = new Schemas();
        //the config Value in ucore is an all in one json config of the schema.xml
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        List<Schema> schemaList = parseJsonSchema.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.SCHEMA_SCHEMA).toJSONString());
        schema.setSchema(schemaList);

        List<DataNode> dataNodeList = parseJsonDataNode.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.DATA_NODE).toJSONString());
        schema.setDataNode(dataNodeList);

        List<DataHost> dataHostList = parseJsonDataHost.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.DATA_HOST).toJSONString());
        schema.setDataHost(dataHostList);

        schema.setVersion(jsonObj.getString(UcorePathUtil.VERSION));

        String path = ResourceUtil.getResourcePathFromRoot(UcorePathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess ucore to object writePath :" + path);

        this.parseSchemaXmlService.parseToXmlWrite(schema, path, "schema");

        LOGGER.info("SchemasLoader notifyProcess ucore to object zk schema      write :" + path + " is success");

    }

    @Override
    public void notifyCluster() throws Exception {
        Schemas schema = this.parseSchemaXmlService.parseXmlToBean(UcorePathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject schemas = new JSONObject();
        schemas.put(UcorePathUtil.VERSION, schema.getVersion());
        schemas.put(UcorePathUtil.SCHEMA_SCHEMA, schema.getSchema());
        schemas.put(UcorePathUtil.DATA_NODE, schema.getDataNode());
        schemas.put(UcorePathUtil.DATA_HOST, schema.getDataHost());
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, schemas.toJSONString());

    }

}
