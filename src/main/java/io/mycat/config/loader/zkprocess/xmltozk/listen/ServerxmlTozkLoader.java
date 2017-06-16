package io.mycat.config.loader.zkprocess.xmltozk.listen;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.Server;
import io.mycat.config.loader.zkprocess.entity.server.System;
import io.mycat.config.loader.zkprocess.entity.server.user.User;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.SystemJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.UserJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.xml.ServerParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 进行从server.xml加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ServerxmlTozkLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息
    */
    private final String currZkPath;

    /**
     * server文件的路径信息
    */
    private static final String SERVER_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "server.xml";


    /**
     * server的xml的转换信息
    */
    private ParseXmlServiceInf<Server> parseServerXMl;

    /**
     * system信息
    */
    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    /**
     * system信息
     */
    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    public ServerxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath() + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {
        // 1,读取本地的xml文件
        Server server = this.parseServerXMl.parseXmlToBean(SERVER_PATH);
        LOGGER.info("ServerxmlTozkLoader notifyProcessxml to zk server Object  :" + server);
        // 将实体信息写入至zk中
        this.xmlTozkServerJson(currZkPath, server);

        LOGGER.info("ServerxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param server server文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void xmlTozkServerJson(String basePath, Server server) throws Exception {
        // 设置默认的节点信息
        String defaultSystem = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey();
        String defaultSystemValue = this.parseJsonSystem.parseBeanToJson(server.getSystem());
        this.checkAndwriteString(basePath, defaultSystem, defaultSystemValue);

        // 设置用户信息
        String userStr = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey();
        String userValueStr = this.parseJsonUser.parseBeanToJson(server.getUser());
        this.checkAndwriteString(basePath, userStr, userValueStr);
    }
}
