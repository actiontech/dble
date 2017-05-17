package io.mycat.config.loader.zkprocess.zktoxml;

import java.io.PrintStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zktoxml.listen.BinDataPathChildrenCacheListener;
import io.mycat.config.loader.zkprocess.zktoxml.listen.EcacheszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.RuleDataPathChildrenCacheListener;
import io.mycat.config.loader.zkprocess.zktoxml.listen.RuleszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.SchemaszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.SequenceTopropertiesLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.ServerzkToxmlLoader;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.ZKUtils;

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
        // 得到基本路径
        String basePath = ZKUtils.getZKBasePath();
        // 获得zk的连接信息
        CuratorFramework zkConn = buildConnection(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_URL));
        String confInitialized = basePath + "confInitialized";
        //init conf if not
        if (zkConn.checkExists().forPath(confInitialized) == null) {
            String confLockPath = basePath + "confLock";
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
                                zkConn.create().creatingParentContainersIfNeeded().forPath(confInitialized);
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
                    zkConn.create().creatingParentContainersIfNeeded().forPath(confInitialized);
                } finally {
                    confLock.release();
                }
            }
        }
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        zkListen.setBasePath(basePath);

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

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 通知所有人
        zkListen.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());

        // 加载watch
        loadZkWatch(zkListen.getWatchPath(), zkConn, zkListen);

        // 创建临时节点
        createTempNode("/mycat/mycat-cluster-1/line", "tmpNode1", zkConn);


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
     * 创建临时节点测试
    * 方法描述
    * @param parent
    * @param node
    * @param zkConn
    * @throws Exception
    * @创建日期 2016年9月20日
    */
    private static void createTempNode(String parent, String node, final CuratorFramework zkConn) throws Exception {

        String path = ZKPaths.makePath(parent, node);

        zkConn.create().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path);

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
				zkListen.notifly(notPath);
				LOGGER.info("ZktoxmlMain runWatch  process path  event over");
			}
		});
		return cache;
	}

    private static CuratorFramework buildConnection(String url) {
    	return ZKUtils.getConnection();
    }
}
