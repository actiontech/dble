package io.mycat.config.loader.zkprocess.xmltozk.listen;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;

/**
 * OthermsgTozkLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class OthermsgTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(OthermsgTozkLoader.class);


    public OthermsgTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        zookeeperListen.addToInit(this);

    }

    @Override
    public boolean notifyProcess() throws Exception {
        String line = KVPathUtil.getOnlinePath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), line);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + line + " success");

        String seqLine = KVPathUtil.getSequencesInstancePath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLine);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLine + " success");

        String seqLeader = KVPathUtil.getSequencesLeaderPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLeader);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLeader + " success");

        String incrSeq = KVPathUtil.getSequencesIncrPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), incrSeq);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + incrSeq + " success");

        String binlogPauseStatusPath = KVPathUtil.getBinlogPauseStatus();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), binlogPauseStatusPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + binlogPauseStatusPath + " success");

        String binlogPauseInstances = KVPathUtil.getBinlogPauseInstance();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), binlogPauseInstances);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + binlogPauseInstances + " success");

        String ddlPath = KVPathUtil.getDDLPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), ddlPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + ddlPath + " success");

        String confStatusPath = KVPathUtil.getConfStatusPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), confStatusPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + confStatusPath + " success");

        String lockBasePathPath = KVPathUtil.getLockBasePath();
        if (this.getCurator().checkExists().forPath(lockBasePathPath) == null) {
            ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), lockBasePathPath);
            LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + lockBasePathPath + " success");
        }
        return true;
    }

}
