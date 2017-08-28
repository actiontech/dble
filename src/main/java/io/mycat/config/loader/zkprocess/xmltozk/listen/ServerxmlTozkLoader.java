package io.mycat.config.loader.zkprocess.xmltozk.listen;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;

/**
 * ServerxmlTozkLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class ServerxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(ServerxmlTozkLoader.class);

    private final String currZkPath;

    private static final String SERVER_PATH = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + "server.xml";


    private ParseXmlServiceInf<Server> parseServerXMl;

    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    private ParseJsonServiceInf<FireWall> parseJsonFireWall = new FireWallJsonParse();

    public ServerxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                               XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfServerPath();
        zookeeperListen.addToInit(this);
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        Server server = this.parseServerXMl.parseXmlToBean(SERVER_PATH);
        LOGGER.info("ServerxmlTozkLoader notifyProcessxml to zk server Object  :" + server);
        this.xmlTozkServerJson(currZkPath, server);

        LOGGER.info("ServerxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * xmlTozkServerJson
     *
     * @param basePath
     * @param server
     * @throws Exception
     * @Created 2016/9/17
     */
    private void xmlTozkServerJson(String basePath, Server server) throws Exception {
        String defaultSystemValue = this.parseJsonSystem.parseBeanToJson(server.getSystem());
        this.checkAndwriteString(basePath, KVPathUtil.DEFAULT, defaultSystemValue);

        String firewallValueStr = this.parseJsonFireWall.parseBeanToJson(server.getFirewall());
        if (firewallValueStr == null) {
            firewallValueStr = "{}";
        }
        this.checkAndwriteString(basePath, KVPathUtil.FIREWALL, firewallValueStr);
        String userValueStr = this.parseJsonUser.parseBeanToJson(server.getUser());
        this.checkAndwriteString(basePath, KVPathUtil.USER, userValueStr);


    }
}
