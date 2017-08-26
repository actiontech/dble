package io.mycat.config.loader.zkprocess.xmltozk.listen;

import io.mycat.config.loader.zkprocess.comm.ConfFileRWUtils;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.Properties;

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
public class SequenceTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTozkLoader.class);

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


    public SequenceTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getSequencesPath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {

        // 将zk序列配配制信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_conf to zk success");

        // 将zk的db方式信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DB_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_db_conf to zk success");

        // 将zk的分布式信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DISTRIBUTED_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_distributed_conf to zk success");


        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     *
     * @param basePath 基本路径
     * @param name     文件的信息
     * @throws Exception 异常信息
     * @Created 2016/9/17
     */
    private void sequenceTozk(String basePath, String name) throws Exception {
        // 读取当前节的信息
        String readFile = name + PROPERTIES_SUFFIX;
        // 读取公共节点的信息
        String commSequence = ConfFileRWUtils.readFile(readFile);
        if (name.equals(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF)) {
            Properties props = new Properties();
            props.load(new StringReader(commSequence));
            if (!"ZK".equals(props.getProperty("INSTANCEID"))) {
                LOGGER.info("The property of INSTANCEID in " + readFile + " is not zk,no need to store in zk");
                return;
            }
        }
        String sequenceZkPath = ZKPaths.makePath(KVPathUtil.SEQUENCE_COMMON, readFile);
        this.checkAndwriteString(basePath, sequenceZkPath, commSequence);
    }
}
