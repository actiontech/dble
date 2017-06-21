package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.BinlogPause;
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.mycat.manager.response.ShowBinlogStatus.*;

/**
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener  extends ZkMultLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);
    private final String currZkPath;

    public BinlogPauseStatusListener(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {

        this.setCurator(curator);
        currZkPath = zookeeperListen.getBasePath() + BINLOG_PAUSE_STATUS;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(zookeeperListen.getBasePath() + KW_BINLOG_PAUSE, this);
        zookeeperListen.watchPath(zookeeperListen.getBasePath() + KW_BINLOG_PAUSE, KW_BINLOG_PAUSE_STATUS);
    }
    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {
        if (isAll) {
            return true;
        }
        String basePath = ZKUtils.getZKBasePath();
        // 通过组合模式进行zk目录树的加载
        DiretoryInf StatusDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, BINLOG_PAUSE_STATUS, StatusDirectory);
        // 从当前的下一级开始进行遍历,获得到
        ZkDataImpl zkDdata = (ZkDataImpl) StatusDirectory.getSubordinateInfo().get(0);
        String strPauseInfo = zkDdata.getDataValue();
        LOGGER.info("BinlogPauseStatusListener notifyProcess zk to object  :" + strPauseInfo);

        BinlogPause pauseInfo  = new BinlogPause(strPauseInfo);
        if(pauseInfo.getFrom().equals(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID))) {
            return true; //self node
        }
        String binlogPause = basePath + ShowBinlogStatus.BINLOG_PAUSE_INSTANCES;
        String instancePath = ZKPaths.makePath(binlogPause, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
        switch (pauseInfo.getStatus()) {
            case ON:
                MycatServer.getInstance().getBackupLocked().compareAndSet(false, true);
                if (ShowBinlogStatus.waitAllSession()) {
                    try {
                        ZKUtils.createTempNode(binlogPause, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
                    } catch (Exception e) {
                        LOGGER.warn("create binlogPause instance failed", e);
                    }
                }
                break;
            case TIMEOUT:
                ShowBinlogStatus.setWaiting(false);
                break;
            case OFF:
                while(ShowBinlogStatus.isWaiting()){
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                }
                try {
                    if (this.getCurator().checkExists().forPath(instancePath) != null) {
                        this.getCurator().delete().forPath(instancePath);
                    }
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
