/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa.recovery.impl;

import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.Deserializer;
import com.actiontech.dble.backend.mysql.xa.Serializer;
import com.actiontech.dble.backend.mysql.xa.recovery.DeserialisationException;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkParamCfg;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huqing.yan on 2017/6/30.
 */
public class KVStoreRepository implements Repository {
    public static final Logger LOGGER = LoggerFactory.getLogger(KVStoreRepository.class);
    private String logPath;
    private CuratorFramework zkConn = ZKUtils.getConnection();

    public KVStoreRepository() {
        init();
    }

    @Override
    public void init() {
        logPath = KVPathUtil.XALOG + ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
    }

    @Override
    public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CoordinatorLogEntry get(String coordinatorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        String data = null;
        try {
            if (zkConn.checkExists().forPath(logPath) != null) {
                try {
                    byte[] raw = zkConn.getData().forPath(logPath);
                    if (raw != null) {
                        data = new String(raw, StandardCharsets.UTF_8);
                    }
                } catch (Exception e) {
                    LOGGER.warn("KVStoreRepository.getAllCoordinatorLogEntries error", e);
                }
            }
        } catch (Exception e2) {
            LOGGER.warn("KVStoreRepository error", e2);
        }
        if (data == null) {
            return Collections.emptyList();
        }
        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
        String[] logs = data.split(Serializer.LINE_SEPARATOR);
        for (String log : logs) {
            try {
                CoordinatorLogEntry coordinatorLogEntry = Deserializer.fromJson(log);
                coordinatorLogEntries.put(coordinatorLogEntry.getId(), coordinatorLogEntry);
            } catch (DeserialisationException e) {
                LOGGER.warn("Unexpected EOF - logfile not closed properly last time? ", e);
            }
        }
        return coordinatorLogEntries.values();
    }

    @Override
    public boolean writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) {
        try {
            StringBuilder sb = new StringBuilder();
            for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
                sb.append(Serializer.toJson(coordinatorLogEntry));
            }
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
            if (zkConn.checkExists().forPath(logPath) == null) {
                zkConn.create().creatingParentsIfNeeded().forPath(logPath);
            }
            zkConn.setData().forPath(logPath, data);
            return true;
        } catch (Exception e) {
            LOGGER.warn("Failed to write checkpoint", e);
            return false;
        }
    }

    @Override
    public void close() {
    }
}
