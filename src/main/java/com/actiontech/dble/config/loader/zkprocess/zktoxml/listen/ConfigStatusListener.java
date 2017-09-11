/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DiretoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.manager.response.ReloadConfig;
import com.actiontech.dble.manager.response.RollbackConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by huqing.yan on 2017/6/23.
 */
public class ConfigStatusListener extends ZkMultLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStatusListener.class);
    public static final String SUCCESS = "SUCCESS";
    private final String currZkPath;
    private Set<NotifyService> childService = new HashSet<>();

    public ConfigStatusListener(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfStatusPath();
        zookeeperListen.addWatch(currZkPath, this);
    }

    public void addChild(NotifyService service) {
        childService.add(service);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        if (DbleServer.getInstance().getProcessors() != null) {
            DiretoryInf statusDirectory = new ZkDirectoryImpl(currZkPath, null);
            this.getTreeDirectory(currZkPath, KVPathUtil.CONF_STATUS, statusDirectory);
            ZkDirectoryImpl zkDdata = (ZkDirectoryImpl) statusDirectory.getSubordinateInfo().get(0);
            ConfStatus status = new ConfStatus(zkDdata.getValue());
            if (status.getFrom().equals(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID))) {
                return true; //self node
            }
            LOGGER.info("ConfigStatusListener notifyProcess zk to object  :" + status);
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                try {
                    RollbackConfig.rollback();
                    ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), SUCCESS.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), errorinfo.getBytes(StandardCharsets.UTF_8));
                }

                return true;
            }
            for (NotifyService service : childService) {
                try {
                    service.notifyProcess();
                } catch (Exception e) {
                    LOGGER.error("ConfigStatusListener notify  error :" + service + " ,Exception info:", e);
                }
            }
            try {
                if (status.getStatus() == ConfStatus.Status.RELOAD_ALL) {
                    ReloadConfig.reloadAll();
                } else {
                    ReloadConfig.reload();
                }
                ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), SUCCESS.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), errorinfo.getBytes(StandardCharsets.UTF_8));
            }
        }
        return true;
    }
}
