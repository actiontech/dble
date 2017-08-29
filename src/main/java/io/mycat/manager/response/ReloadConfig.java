/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.config.ConfigInitializer;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain;
import io.mycat.config.loader.zkprocess.zktoxml.listen.ConfigStatusListener;
import io.mycat.config.model.ERTable;
import io.mycat.config.model.FirewallConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.config.util.DnPropertyUtil;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.OkPacket;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 * @author zhuam
 */
public final class ReloadConfig {
    private ReloadConfig() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadConfig.class);

    public static void execute(ManagerConnection c, final boolean loadAll) {
        // reload @@config_all check the last old connections
        if (loadAll && !NIOProcessor.BACKENDS_OLD.isEmpty()) {
            c.writeErrMessage(ErrorCode.ER_YES, "The before reload @@config_all has an unfinished db transaction, please try again later.");
            return;
        }
        if (MycatServer.getInstance().isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getConfChangeLockPath());
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                } else {
                    try {
                        final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
                        lock.lock();
                        try {
                            if (loadAll) {
                                reloadAll();
                            } else {
                                reload();
                            }
                            //tell zk this instance has prepared
                            ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
                            XmltoZkMain.writeConfFileToZK(loadAll);
                            //check all session waiting status
                            List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
                            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                            // TODO: While waiting, a new instance of MyCat is upping and working.
                            while (preparedList.size() < onlineList.size()) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                                preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
                            }
                            StringBuilder sbErrorInfo = new StringBuilder();
                            for (String child : preparedList) {
                                String childPath = ZKPaths.makePath(KVPathUtil.getConfStatusPath(), child);
                                byte[] errorInfo = zkConn.getData().forPath(childPath);
                                if (!ConfigStatusListener.SUCCESS.equals(new String(errorInfo, StandardCharsets.UTF_8))) {
                                    sbErrorInfo.append(child + ":");
                                    sbErrorInfo.append(new String(errorInfo, StandardCharsets.UTF_8));
                                    sbErrorInfo.append(";");
                                }
                                zkConn.delete().forPath(ZKPaths.makePath(KVPathUtil.getConfStatusPath(), child));
                            }
                            if (sbErrorInfo.length() == 0) {
                                writeOKResult(c);
                            } else {
                                writeErrorResult(c, sbErrorInfo.toString());
                            }
                        } catch (Exception e) {
                            LOGGER.warn("reload config failure", e);
                            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
                        } finally {
                            lock.unlock();
                        }
                    } finally {
                        distributeLock.release();
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("reload config failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else {
            final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
            lock.lock();
            try {
                try {
                    if (loadAll) {
                        reloadAll();
                    } else {
                        reload();
                    }
                    writeOKResult(c);
                } catch (Exception e) {
                    writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private static void writeOKResult(ManagerConnection c) {
        LOGGER.info("send ok package to client " + String.valueOf(c));
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Reload config success".getBytes());
        ok.write(c);
    }

    private static void writeErrorResult(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb + "." + String.valueOf(c));
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static void reloadAll() throws Exception {
        /*
         *  1 load new conf
         *  1.1 ConfigInitializer init adn check itself
         *  1.2 DataNode/DataHost test connection
         */
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(true);
        } catch (Exception e) {
            throw new Exception(e);
        }
        Map<String, UserConfig> newUsers = loader.getUsers();
        Map<String, SchemaConfig> newSchemas = loader.getSchemas();
        Map<String, PhysicalDBNode> newDataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> newDataHosts = loader.getDataHosts();
        Map<ERTable, Set<ERTable>> newErRelations = loader.getErRelations();
        FirewallConfig newFirewall = loader.getFirewall();

        try {
            loader.testConnection();
        } catch (Exception e) {
            throw new Exception(e);
        }

        /*
         *  2 transform
         *  2.1 old dataSource continue to work
         *  2.2 init the new dataSource
         *  2.3 transform
         *  2.4  put the old connection into a queue
         */
        MycatConfig config = MycatServer.getInstance().getConfig();

        /* 2.1 do nothing */
        boolean isReloadStatusOK = true;

        /* 2.2 init the new dataSource */
        for (PhysicalDBPool dbPool : newDataHosts.values()) {
            String hostName = dbPool.getHostName();
            // set schemas
            ArrayList<String> dnSchemas = new ArrayList<>(30);
            for (PhysicalDBNode dn : newDataNodes.values()) {
                if (dn.getDbPool().getHostName().equals(hostName)) {
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbPool.setSchemas(dnSchemas.toArray(new String[dnSchemas.size()]));

            // get data host
            String dnIndex = DnPropertyUtil.loadDnIndexProps().getProperty(dbPool.getHostName(), "0");
            if (!"0".equals(dnIndex)) {
                LOGGER.info("init datahost: " + dbPool.getHostName() + " to use datasource index:" + dnIndex);
            }

            dbPool.init(Integer.parseInt(dnIndex));
            if (!dbPool.isInitSuccess()) {
                isReloadStatusOK = false;
                break;
            }
        }

        if (isReloadStatusOK) {
            /* 2.3 apply new conf */
            config.reload(newUsers, newSchemas, newDataNodes, newDataHosts, newErRelations, newFirewall, loader.isDataHostWithoutWH(), true);

            /* 2.4 put the old connection into a queue */
            Map<String, PhysicalDBPool> oldDataHosts = config.getBackupDataHosts();
            for (PhysicalDBPool dbPool : oldDataHosts.values()) {
                dbPool.stopHeartbeat();

                for (PhysicalDatasource ds : dbPool.getAllDataSources()) {
                    for (NIOProcessor processor : MycatServer.getInstance().getProcessors()) {
                        for (BackendConnection con : processor.getBackends().values()) {
                            if (con instanceof MySQLConnection) {
                                MySQLConnection mysqlCon = (MySQLConnection) con;
                                if (mysqlCon.getPool() == ds && con.isBorrowed()) {
                                    NIOProcessor.BACKENDS_OLD.add(con);
                                }
                            }
                        }
                    }
                }
            }
            LOGGER.info("the size of old backend connection to be recycled is: " + NIOProcessor.BACKENDS_OLD.size());

        } else {
            // INIT FAILED
            LOGGER.info("reload failed, clear previously created datasources ");
            for (PhysicalDBPool dbPool : newDataHosts.values()) {
                dbPool.clearDataSources("reload config");
                dbPool.stopHeartbeat();
            }
            throw new Exception("Init DbPool failed");
        }
    }

    public static void reload() throws Exception {
        /* 1 load new conf, ConfigInitializer will check itself */
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(false);
        } catch (Exception e) {
            throw new Exception(e);
        }
        Map<String, UserConfig> users = loader.getUsers();
        Map<String, SchemaConfig> schemas = loader.getSchemas();
        Map<String, PhysicalDBNode> dataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> dataHosts = loader.getDataHosts();
        Map<ERTable, Set<ERTable>> erRelations = loader.getErRelations();
        FirewallConfig firewall = loader.getFirewall();

        /* 2 apply the new conf */
        MycatServer.getInstance().getConfig().reload(users, schemas, dataNodes, dataHosts, erRelations, firewall, loader.isDataHostWithoutWH(), false);
    }

}
