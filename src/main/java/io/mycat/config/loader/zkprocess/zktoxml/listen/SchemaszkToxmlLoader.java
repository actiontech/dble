package io.mycat.config.loader.zkprocess.zktoxml.listen;

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
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * 进行schema的文件从zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SchemaszkToxmlLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaszkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息
    */
    private final String currZkPath;

    /**
     * 写入本地的文件路径
    */
    private static final String WRITEPATH = "schema.xml";

    /**
     * schema类与xml转换服务
    */
    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    /**
     * 进行将schema
    */
    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    /**
     * 进行将dataNode
     */
    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    /**
     * 进行将dataNode
     */
    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();


    public SchemaszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfSchemaPath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit( this);
        // 生成xml与类的转换信息
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        // 1,将集群schema目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf schemaDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, KVPathUtil.SCHEMA, schemaDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) schemaDirectory.getSubordinateInfo().get(0);

        Schemas schema = this.zktoSchemasBean(zkDirectory);

        LOGGER.info("SchemasLoader notifyProcess zk to object  zk schema Object  :" + schema);

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path=new File(path).getPath()+File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess zk to object writePath :" + path);

        this.parseSchemaXmlService.parseToXmlWrite(schema, path, "schema");

        LOGGER.info("SchemasLoader notifyProcess zk to object zk schema      write :" + path + " is success");

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
    */
    private Schemas zktoSchemasBean(ZkDirectoryImpl zkDirectory) {
        Schemas schema = new Schemas();

        // 得到schema对象的目录信息
        DataInf schemaZkDirectory = this.getZkData(zkDirectory, KVPathUtil.SCHEMA_SCHEMA);
        List<Schema> schemaList = parseJsonSchema.parseJsonToBean(schemaZkDirectory.getDataValue());
        schema.setSchema(schemaList);


        // 得到dataNode的信息
        DataInf dataNodeZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DATA_NODE);
        List<DataNode> dataNodeList = parseJsonDataNode.parseJsonToBean(dataNodeZkDirectory.getDataValue());
        schema.setDataNode(dataNodeList);


        // 得到dataNode的信息
        DataInf dataHostZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DATA_HOST);
        List<DataHost> dataHostList = parseJsonDataHost.parseJsonToBean(dataHostZkDirectory.getDataValue());
        schema.setDataHost(dataHostList);


        return schema;

    }

}
