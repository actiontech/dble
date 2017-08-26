package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ConfFileRWUtils;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * 进行从ecache.xml加载到zk中加载
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class EcacheszkToxmlLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(EcacheszkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息
     */
    private final String currZkPath;

    /**
     * ehcache的xml的转换信息
     */
    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    /**
     * 表的路由信息
     */
    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    /**
     * 监控类信息
     */
    private ZookeeperProcessListen zookeeperListen;

    public EcacheszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        this.zookeeperListen = zookeeperListen;
        currZkPath = KVPathUtil.getCachePath();
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addToInit(this);
        // 生成xml与类的转换信息
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {

        // 通过组合模式进行zk目录树的加载
        DiretoryInf rulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, KVPathUtil.CACHE, rulesDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) rulesDirectory.getSubordinateInfo().get(0);

        // 进行写入操作
        zktoEhcacheWrite(zkDirectory);

        LOGGER.info("EcacheszkToxmlLoader notifyProcess   zk ehcache write success ");

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
     * 方法描述
     *
     * @param zkDirectory
     * @return
     * @Created 2016/9/17
     */
    private void zktoEhcacheWrite(ZkDirectoryImpl zkDirectory) {

        // 得到schema对象的目录信息
        DataInf ehcacheZkDirectory = this.getZkData(zkDirectory, KVPathUtil.EHCACHE_NAME);

        Ehcache ehcache = parseJsonEhcacheService.parseJsonToBean(ehcacheZkDirectory.getDataValue());

        String outputPath = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        outputPath = new File(outputPath).getPath() + File.separator;
        outputPath += KVPathUtil.EHCACHE_NAME;

        parseEcacheXMl.parseToXmlWrite(ehcache, outputPath, null);

        // 设置zk监控
        this.zookeeperListen.addWatch(KVPathUtil.getEhcacheNamePath(), this);

        // 写入cacheservice.properties的信息
        DataInf cacheserZkDirectory = this.getZkData(zkDirectory, KVPathUtil.CACHESERVER_NAME);

        if (null != cacheserZkDirectory) {
            ZkDataImpl cacheData = (ZkDataImpl) cacheserZkDirectory;

            // 写入文件cacheservice.properties
            try {
                ConfFileRWUtils.writeFile(cacheData.getName(), cacheData.getValue());
            } catch (IOException e) {
                LOGGER.error("EcacheszkToxmlLoader wirteMapFile IOException", e);
            }
            this.zookeeperListen.addWatch(KVPathUtil.getCacheServerNamePath(), this);
        }

    }
}
