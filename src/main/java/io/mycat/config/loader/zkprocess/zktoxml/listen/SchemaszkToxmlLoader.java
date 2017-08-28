package io.mycat.config.loader.zkprocess.zktoxml.listen;

import java.io.File;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.Schemas;
import io.mycat.config.loader.zkprocess.entity.schema.datahost.DataHost;
import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataHostJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataNodeJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.SchemaJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ResourceUtil;

/**
 * SchemaszkToxmlLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
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
