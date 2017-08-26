package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.config.loader.zkprocess.comm.ConfFileRWUtils;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进行从sequence加载到zk中加载
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class SequenceTopropertiesLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTopropertiesLoader.class);

    /**
     * 当前文件中的zkpath信息
     */
    private final String currZkPath;

    /**
     * 后缀名
     */
    private static final String PROPERTIES_SUFFIX = ".properties";

    /**
     * 序列配制信息
     */
    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    /**
     * db序列配制信息
     */
    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    /**
     * 分布式的序列配制
     */
    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";


    /**
     * 监控路径信息
     */
    private ZookeeperProcessListen zookeeperListen;

    public SequenceTopropertiesLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        this.zookeeperListen = zookeeperListen;
        currZkPath = KVPathUtil.getSequencesPath();
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {

        // 1,将集群server目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf sequenceDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, KVPathUtil.SEQUENCES, sequenceDirectory);

        // 取到当前根目录 信息
        sequenceDirectory = (DiretoryInf) sequenceDirectory.getSubordinateInfo().get(0);

        // 将zk序列配配制信息入本地文件
        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_CONF, sequenceDirectory);

        // 将zk的db方式信息入本地文件
        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_DB_CONF, sequenceDirectory);

        // 将zk的分布式信息入本地文件
        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF, sequenceDirectory);
        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     *
     * @param name schema文件的信息
     * @throws Exception 异常信息
     */
    private void sequenceZkToProperties(String name, DiretoryInf seqDirectory) throws Exception {
        // 读取当前节的信息
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory, KVPathUtil.SEQUENCE_COMMON);

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            // 读取common目录下的数据
            ZkDataImpl commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            if (commData != null) {
                ConfFileRWUtils.writeFile(commData.getName(), commData.getValue());
            }
            String sequenceWatchPath = KVPathUtil.getSequencesCommonPath() + writeFile;
            this.zookeeperListen.addWatch(sequenceWatchPath, this);
            LOGGER.info("SequenceTozkLoader notifyProcess " + name + " to local properties success");
        }
    }

}
