package io.mycat.config.loader.zkprocess.zktoxml;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain;
import io.mycat.config.loader.zkprocess.zktoxml.listen.*;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 将xk的信息转换为xml文件的操作
* 源文件名：ZktoxmlMain.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月20日
* 修改作者：liujun
* 修改日期：2016年9月20日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZktoXmlMain {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkMultLoader.class);
    public static void main(String[] args) throws Exception {
        loadZktoFile();
        System.out.println("ZktoXmlMain Finished");
    }

    /**
     * 将zk数据放到到本地
    * 方法描述
     * @throws Exception 
     * @创建日期 2016年9月21日
    */
    public static void loadZktoFile() throws Exception {
        // 获得zk的连接信息
        CuratorFramework zkConn = ZKUtils.getConnection();
        //if first start,init zk
        initZKIfNot(zkConn);
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        initLocalConfFromZK(zkListen, zkConn);
        // 加载watch
        loadZkWatch(zkListen.getWatchPath(), zkConn, zkListen);

    }

    private static void initLocalConfFromZK(ZookeeperProcessListen zkListen, CuratorFramework zkConn) throws JAXBException {

        ConfigStatusListener confListener = new ConfigStatusListener(zkListen,zkConn);
        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 加载以接收者
        new SchemaszkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // server加载
        new ServerzkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // rule文件加载
        new RuleszkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // 将序列配制信息加载
        new SequenceTopropertiesLoader(zkListen, zkConn);

        // 进行ehcache转换
        new EcacheszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // 将bindata目录的数据进行转换到本地文件
        ZKUtils.addChildPathCache(KVPathUtil.getBinDataPath(),new BinDataPathChildrenCacheListener());

        new BinlogPauseStatusListener(zkListen, zkConn);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 通知所有人
        zkListen.initAllNode();
        zkListen.clearInited();
        if (MycatServer.getInstance().getProcessors() != null) {
            ReloadConfig.reload_all();
        }
    }

    private static void initZKIfNot(CuratorFramework zkConn) throws Exception {
        String confInited =  KVPathUtil.getConfInitedPath();
        //init conf if not
        if (zkConn.checkExists().forPath(confInited) == null) {
            InterProcessMutex confLock = new InterProcessMutex(zkConn, KVPathUtil.getConfInitLockPath());
            //someone acquired the lock
            if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                //loop wait for initialized
                while (true) {
                    if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    } else {
                        try {
                            if (zkConn.checkExists().forPath(confInited) == null) {
                                XmltoZkMain.initFileToZK();
                            }
                            break;
                        } finally {
                            confLock.release();
                        }
                    }
                }
            } else {
                try {
                    XmltoZkMain.initFileToZK();
                } finally {
                    confLock.release();
                }
            }
        }
    }

    private static void loadZkWatch(Set<String> setPaths, final CuratorFramework zkConn,
                                    final ZookeeperProcessListen zkListen) throws Exception {
        if (null != setPaths && !setPaths.isEmpty()) {
            for (String path : setPaths) {
                final NodeCache node = new NodeCache(zkConn, path);
                node.start(true);
                runWatch(node, zkListen);
                LOGGER.info("ZktoxmlMain loadZkWatch path:" + path + " regist success");
            }
        }
    }

    /**
     * 进行zk的watch操作
    * 方法描述
    * @param cache NodeCache
    * @param zkListen 监控路径信息
    * @throws Exception
    * @创建日期 2016年9月20日
    */
    private static void runWatch(final NodeCache cache, final ZookeeperProcessListen zkListen)
            throws Exception {
        cache.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() {
                LOGGER.info("ZktoxmlMain runWatch  process path  event start ");
                String notPath = cache.getCurrentData().getPath();
                LOGGER.info("NodeCache changed, path is: " + notPath);
                // 进行通知更新
                zkListen.notify(notPath);
                LOGGER.info("ZktoxmlMain runWatch  process path  event over");
            }
        });
    }
}
