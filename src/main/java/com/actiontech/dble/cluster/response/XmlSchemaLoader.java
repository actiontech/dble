/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
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
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Type;
import java.util.List;

import static com.actiontech.dble.backend.datasource.PhysicalDataHost.JSON_LIST;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlSchemaLoader implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlSchemaLoader.class);

    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();

    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    private static final String WRITEPATH = "schema.xml";

    private static final String CONFIG_PATH = ClusterPathUtil.getConfSchemaPath();


    public XmlSchemaLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Schemas schema = new Schemas();
        //the config Value in ucore is an all in one json config of the schema.xml
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        List<Schema> schemaList = parseJsonSchema.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.SCHEMA_SCHEMA).toJSONString());
        schema.setSchema(schemaList);

        List<DataNode> dataNodeList = parseJsonDataNode.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.DATA_NODE).toJSONString());
        schema.setDataNode(dataNodeList);

        List<DataHost> dataHostList = parseJsonDataHost.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.DATA_HOST).toJSONString());
        schema.setDataHost(dataHostList);
        if (ClusterHelper.useClusterHa()) {
            List<KvBean> statusKVList = ClusterHelper.getKVPath(ClusterPathUtil.getHaStatusPath());
            if (statusKVList != null && statusKVList.size() > 0) {
                for (KvBean kv : statusKVList) {
                    String[] path = kv.getKey().split("/");
                    String dataHostName = path[path.length - 1];
                    for (DataHost dataHost : dataHostList) {
                        if (dataHost.getName().equals(dataHostName)) {
                            JSONObject obj = JSONObject.parseObject(kv.getValue());
                            JsonProcessBase base = new JsonProcessBase();
                            Type parseType = new TypeToken<List<DataSourceStatus>>() {
                            }.getType();
                            List<DataSourceStatus> list = base.toBeanformJson(obj.getJSONArray(JSON_LIST).toJSONString(), parseType);
                            ClusterHelper.changeDataHostByStatus(dataHost, list);
                        }
                    }
                }
            }
        }

        schema.setVersion(jsonObj.getString(ClusterPathUtil.VERSION));

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess ucore to object writePath :" + path);

        this.parseSchemaXmlService.parseToXmlWrite(schema, path, "schema");

        LOGGER.info("SchemasLoader notifyProcess ucore to object zk schema      write :" + path + " is success");

    }

    @Override
    public void notifyCluster() throws Exception {
        Schemas schema = this.parseSchemaXmlService.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject schemas = new JSONObject();
        schemas.put(ClusterPathUtil.VERSION, schema.getVersion());
        schemas.put(ClusterPathUtil.SCHEMA_SCHEMA, schema.getSchema());
        schemas.put(ClusterPathUtil.DATA_NODE, schema.getDataNode());
        schemas.put(ClusterPathUtil.DATA_HOST, schema.getDataHost());
        ClusterHelper.setKV(CONFIG_PATH, schemas.toJSONString());
    }

}
