/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datanode.DataNode;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.schema.Schema;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.DataHostJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.DataNodeJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json.SchemaJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DirectoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Type;
import java.util.List;

import static com.actiontech.dble.backend.datasource.PhysicalDataHost.JSON_LIST;
import static com.actiontech.dble.backend.datasource.PhysicalDataHost.JSON_NAME;

/**
 * SchemaszkToxmlLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SchemaszkToxmlLoader extends ZkMultiLoader implements NotifyService {


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
        DirectoryInf schemaDirectory = new ZkDirectoryImpl(currZkPath, null);
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
        try {
            if (ClusterHelper.useClusterHa()) {
                List<String> chindrenList = getCurator().getChildren().forPath(KVPathUtil.getHaStatusPath());
                if (chindrenList != null && chindrenList.size() > 0) {
                    for (String child : chindrenList) {
                        String data = new String(getCurator().getData().forPath(ZKPaths.makePath(KVPathUtil.getHaStatusPath() + ZKPaths.PATH_SEPARATOR, child)), "UTF-8");
                        JSONObject jsonObj = JSONObject.parseObject(data);
                        JsonProcessBase base = new JsonProcessBase();
                        Type parseType = new TypeToken<List<DataSourceStatus>>() {
                        }.getType();
                        String dataHostName = jsonObj.getString(JSON_NAME);
                        List<DataSourceStatus> list = base.toBeanformJson(jsonObj.getJSONArray(JSON_LIST).toJSONString(), parseType);
                        for (DataHost dataHost : dataHostList) {
                            if (dataHost.getName().equals(dataHostName)) {
                                ClusterHelper.changeDataHostByStatus(dataHost, list);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("get error try to write schema.xml");
        }

        DataInf version = this.getZkData(zkDirectory, KVPathUtil.VERSION);
        schema.setVersion(version == null ? null : version.getDataValue());
        return schema;
    }


}
