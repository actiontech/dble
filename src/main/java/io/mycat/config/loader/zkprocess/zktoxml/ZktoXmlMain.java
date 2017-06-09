package io.mycat.config.loader.zkprocess.zktoxml;

import io.mycat.MycatServer;
import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ZkNotifyCfg;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain;
import io.mycat.config.loader.zkprocess.zktoxml.listen.*;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;
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
        initZKIfNot( zkConn);
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        zkListen.setBasePath(ZKUtils.getZKBasePath());
        initLocalConfFromZK(zkListen, zkConn);
        // 加载watch
        loadZkWatch(zkListen.getWatchPath(), zkConn, zkListen);
        // 创建online状态
        ZKUtils.createTempNode(ZKUtils.getZKBasePath()+ZookeeperPath.FLOW_ZK_PATH_ONLINE.getKey(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));

    }

    private static void initLocalConfFromZK(ZookeeperProcessListen zkListen, CuratorFramework zkConn) throws JAXBException {
        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 加载以接收者
        new SchemaszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // server加载
        new ServerzkToxmlLoader(zkListen, zkConn, xmlProcess);

        // rule文件加载
        new RuleszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // 将序列配制信息加载
        new SequenceTopropertiesLoader(zkListen, zkConn, xmlProcess);

        // 进行ehcache转换
        new EcacheszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // 将bindata目录的数据进行转换到本地文件
        ZKUtils.addChildPathCache(ZKUtils.getZKBasePath()+"bindata",new BinDataPathChildrenCacheListener());

        //ruledata
        ZKUtils.addChildPathCache(ZKUtils.getZKBasePath()+"ruledata",new RuleDataPathChildrenCacheListener());
        //
        new BinlogPauseStatusListener(zkListen, zkConn);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 通知所有人
        zkListen.notify(ZkNotifyCfg.ZK_NOTIFY_LOAD_ALL.getKey());
        if (MycatServer.getInstance().getProcessors() != null) {
            ReloadConfig.reload_all();
        }
    }

    private static void initZKIfNot(CuratorFramework zkConn) throws Exception {
        String basePath = ZKUtils.getZKBasePath();
        String confInitialized = basePath + ZookeeperPath.ZK_CONF_INITED.getKey();
        //init conf if not
        if (zkConn.checkExists().forPath(confInitialized) == null) {
            String confLockPath = basePath + "lock/confInit.lock";
            InterProcessMutex confLock = new InterProcessMutex(zkConn, confLockPath);
            //someone acquired the lock
            if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                //loop wait for initialized
                while (true) {
                    if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    } else {
                        try {
                            if (zkConn.checkExists().forPath(confInitialized) == null) {
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
                NodeCache node = runWatch(zkConn, path, zkListen);
                node.start();
                LOGGER.info("ZktoxmlMain loadZkWatch path:" + path + " regist success");
            }
        }
    }

    /**
     * 进行zk的watch操作
    * 方法描述
    * @param zkConn zk的连接信息
    * @param path 路径信息
    * @param zkListen 监控路径信息
    * @throws Exception
    * @创建日期 2016年9月20日
    */
	private static NodeCache runWatch(final CuratorFramework zkConn, String path, final ZookeeperProcessListen zkListen)
			throws Exception {
		final NodeCache cache = new NodeCache(zkConn, path);
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
		return cache;
	}
}
