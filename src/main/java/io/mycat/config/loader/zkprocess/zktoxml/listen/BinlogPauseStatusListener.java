package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ShowBinlogStatus;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.manager.response.ShowBinlogStatus.*;

/**
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener  extends ZkMultLoader implements NotiflyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);
    private final String currZkPath;

    public BinlogPauseStatusListener(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {

        this.setCurator(curator);

        String  binlogPauseStatusPath = zookeeperListen.getBasePath() + BINLOG_PAUSE_STATUS;

        currZkPath = binlogPauseStatusPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(zookeeperListen.getBasePath() + KW_BINLOG_PAUSE, this);
        zookeeperListen.watchPath(zookeeperListen.getBasePath() + KW_BINLOG_PAUSE, KW_BINLOG_PAUSE_STATUS);
    }
    @Override
    public boolean notiflyProcess() throws Exception {
        if (MycatServer.getInstance().getProcessors() == null) {
            return true;
        }
        String basePath = ZKUtils.getZKBasePath();
        String lockPath = basePath + BINLOG_PAUSE_LOCK;
        InterProcessMutex distributeLock = new InterProcessMutex(this.getCurator(), lockPath);
        if(distributeLock.isAcquiredInThisProcess()){
            return true;
        }
        // 1,将集群Rules目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf StatusDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, BINLOG_PAUSE_STATUS, StatusDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDataImpl zkDdata = (ZkDataImpl) StatusDirectory.getSubordinateInfo().get(0);
        String strStatus = zkDdata.getDataValue();
        LOGGER.info("BinlogPauseStatusListener notiflyProcess zk to object  :" + strStatus);

        String instancePath = ZKPaths.makePath(basePath + ShowBinlogStatus.BINLOG_PAUSE_INSTANCES,ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
        ShowBinlogStatus.BinlogPauseStatus status =ShowBinlogStatus.BinlogPauseStatus.valueOf(strStatus);
        switch (status) {
            case ON:
                MycatServer.getInstance().getBackupLocked().compareAndSet(false, true);
                ShowBinlogStatus.waitAllSession();
                String binlogPause = basePath + ShowBinlogStatus.BINLOG_PAUSE_INSTANCES;
                try {
                    ZKUtils.createTempNode(binlogPause, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
                } catch (Exception e) {
                    LOGGER.warn("create binlogPause instance failed", e);
                }
                break;
            case OFF:
                try {
                    this.getCurator().delete().forPath(instancePath);
                } catch (Exception e) {
                    LOGGER.warn("delete binlogPause instance failed", e);
                } finally {
                    MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
                }
                break;
        }


        return true;
    }
}
