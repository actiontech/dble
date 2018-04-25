package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean.DELETE;

/**
 * Created by szf on 2018/4/24.
 */
public class UPauseDataNodeResponse implements UcoreXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UPauseDataNodeResponse.class);

    private static final String CONFIG_PATH = UcorePathUtil.getPauseDataNodePath();

    private Thread waitThread;

    public UPauseDataNodeResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        if (!DELETE.equals(configValue.getChangeType())) {
            if (CONFIG_PATH.split("/").length == configValue.getKey().split("/").length) {
                PauseInfo pauseInfo = new PauseInfo(configValue.getValue());
                if (pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                    return;
                } else {
                    if (PauseInfo.PAUSE.equals(pauseInfo.getType())) {
                        final String dataNodes = pauseInfo.getDataNodes();
                        waitThread = new Thread(new Runnable() {
                            @Override
                            public void run() {

                                try {
                                    Set<String> dataNodeSet = new HashSet(Arrays.asList(dataNodes.split(",")));
                                    DbleServer.getInstance().getMiManager().getIsPausing().set(true);
                                    DbleServer.getInstance().getMiManager().lockWithDataNodes(dataNodeSet);

                                    while (true) {
                                        boolean nextTurn = false;
                                        for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                                            for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                                                if (entry.getValue() instanceof ServerConnection) {
                                                    ServerConnection sconnection = (ServerConnection) entry.getValue();
                                                    for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : sconnection.getSession2().getTargetMap().entrySet()) {
                                                        if (dataNodeSet.contains(conEntry.getKey().getName())) {
                                                            nextTurn = true;
                                                            break;
                                                        }
                                                    }
                                                    if (nextTurn) {
                                                        break;
                                                    }
                                                }
                                            }
                                            if (nextTurn) {
                                                break;
                                            }
                                        }
                                        if (!nextTurn) {
                                            break;
                                        }
                                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                                    }

                                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getPauseResultNodePath(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                                            UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));

                                } catch (Exception e) {
                                    LOGGER.warn(AlarmCode.CORE_CLUSTER_WARN + " the ucore pause error " + e.getMessage());
                                }

                            }
                        });
                        waitThread.start();
                    } else {

                        if (waitThread.isAlive()) {
                            waitThread.interrupt();
                        }
                        DbleServer.getInstance().getMiManager().resume();
                        ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getPauseResumePath(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                                UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));

                    }
                }
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
