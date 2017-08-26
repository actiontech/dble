package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.Server;
import io.mycat.config.loader.zkprocess.entity.server.FireWall;
import io.mycat.config.loader.zkprocess.entity.server.System;
import io.mycat.config.loader.zkprocess.entity.server.User;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.FireWallJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.SystemJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.UserJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.xml.ServerParseXmlImpl;
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
 * 进行server的文件从zk中加载
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class ServerzkToxmlLoader extends ZkMultLoader implements NotifyService {


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
     * user信息
     */
    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    public ServerzkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                               XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfServerPath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit(this);
        // 生成xml与类的转换信息
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        // 1,将集群server目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf serverDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, KVPathUtil.SERVER, serverDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) serverDirectory.getSubordinateInfo().get(0);
        Server server = this.zktoServerBean(zkDirectory);

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object  zk server Object  :" + server);

        // 数配制信息写入文件
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object writePath :" + path);

        this.parseServerXMl.parseToXmlWrite(server, path, "server");

        LOGGER.info("ServerzkToxmlLoader notifyProcess zk to object zk server      write :" + path + " is success");

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
     * 方法描述
     *
     * @param zkDirectory
     * @return
     * @Created 2016/9/17
     * @create
     */
    private Server zktoServerBean(DiretoryInf zkDirectory) {
        Server server = new Server();

        // 得到server对象的目录信息
        DataInf serverZkDirectory = this.getZkData(zkDirectory, KVPathUtil.DEFAULT);
        System systemValue = parseJsonSystem.parseJsonToBean(serverZkDirectory.getDataValue());
        server.setSystem(systemValue);

        // 得到firewall的信息
        DataInf firewallZkDirectory = this.getZkData(zkDirectory, KVPathUtil.FIREWALL);
        FireWall fireWall = parseJsonFireWall.parseJsonToBean(firewallZkDirectory.getDataValue());
        server.setFirewall(fireWall);

        // 得到user的信息
        DataInf userZkDirectory = this.getZkData(zkDirectory, KVPathUtil.USER);
        List<User> userList = parseJsonUser.parseJsonToBean(userZkDirectory.getDataValue());
        server.setUser(userList);

        return server;
    }


}
