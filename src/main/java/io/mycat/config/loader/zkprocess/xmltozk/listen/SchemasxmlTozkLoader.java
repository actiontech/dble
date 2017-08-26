package io.mycat.config.loader.zkprocess.xmltozk.listen;

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
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 进行从xml加载到zk中加载
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class SchemasxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SchemasxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息
     */
    private final String currZkPath;

    /**
     * schema文件的路径信息
     */
    private static final String SCHEMA_PATH = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + "schema.xml";

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

    public SchemasxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfSchemaPath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit(this);
        // 生成xml与类的转换信息
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        // 1,读取本地的xml文件
        Schemas schema = this.parseSchemaXmlService.parseXmlToBean(SCHEMA_PATH);

        LOGGER.info("SchemasxmlTozkLoader notifyProcess xml to zk schema Object  :" + schema);

        // 将实体信息写入至zk中
        this.xmlTozkSchemasJson(currZkPath, schema);

        LOGGER.info("SchemasxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     *
     * @param basePath 基本路径
     * @param schema   schema文件的信息
     * @throws Exception 异常信息
     * @Created 2016/9/17
     */
    private void xmlTozkSchemasJson(String basePath, Schemas schema) throws Exception {

        // 设置schema目录的值

        String schemaValueStr = this.parseJsonSchema.parseBeanToJson(schema.getSchema());

        this.checkAndwriteString(basePath, KVPathUtil.SCHEMA_SCHEMA, schemaValueStr);
        // 设置datanode

        String dataNodeValueStr = this.parseJsonDataNode.parseBeanToJson(schema.getDataNode());

        this.checkAndwriteString(basePath, KVPathUtil.DATA_NODE, dataNodeValueStr);

        // 设置dataHost

        String dataHostValueStr = this.parseJsonDataHost.parseBeanToJson(schema.getDataHost());

        this.checkAndwriteString(basePath, KVPathUtil.DATA_HOST, dataHostValueStr);

    }

}
