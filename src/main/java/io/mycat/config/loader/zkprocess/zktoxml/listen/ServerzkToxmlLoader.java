package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.manager.response.ReloadConfig;
import io.mycat.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
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
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * 进行server的文件从zk中加载
* 源文件名：ServerzkToxmlLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ServerzkToxmlLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerzkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息
    */
    private final String currZkPath;

    /**
     * 写入本地的文件路径
    */
    private static final String WRITEPATH = "server.xml";


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

    /**
     * zk监控路径
    */
    private ZookeeperProcessListen zookeeperListen;

    public ServerzkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String serverPath = zookeeperListen.getBasePath() + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey();

        currZkPath = serverPath;
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addListen(serverPath, this);

        // 生成xml与类的转换信息
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {
        // 1,将集群server目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf serverDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey(), serverDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) serverDirectory.getSubordinateInfo().get(0);
        Server server = this.zktoServerBean(zkDirectory);

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object  zk server Object  :" + server);

        // 数配制信息写入文件
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path=new File(path).getPath()+File.separator;
        path += WRITEPATH;

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object writePath :" + path);

        this.parseServerXMl.parseToXmlWrite(server, path, "server");

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object zk server      write :" + path + " is success");


        if (!isAll && MycatServer.getInstance().getProcessors() != null)
            ReloadConfig.reload();
        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
    * 方法描述
    * @param zkDirectory
    * @return
    * @创建日期 2016年9月17日
    */
    private Server zktoServerBean(DiretoryInf zkDirectory) {
        Server server = new Server();

        // 得到server对象的目录信息
        DataInf serverZkDirectory = this.getZkData(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey());
        System systemValue = parseJsonSystem.parseJsonToBean(serverZkDirectory.getDataValue());
        server.setSystem(systemValue);

        this.zookeeperListen.watchPath(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey());

        // 得到user的信息
        DataInf userZkDirectory = this.getZkData(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey());
        List<User> userList = parseJsonUser.parseJsonToBean(userZkDirectory.getDataValue());
        server.setUser(userList);

        // 用户路径的监控
        this.zookeeperListen.watchPath(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey());

        return server;
    }



}
