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
        // reload @@config_all 校验前一次的事务完成情况
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
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reload config success".getBytes();
        ok.write(c);
    }

    private static void writeErrorResult(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb + "." + String.valueOf(c));
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static void reloadAll() throws Exception {
        /*
         *  1、载入新的配置
         *  1.1、ConfigInitializer 初始化l, 基本自检
         *  1.2、DataNode/DataHost 实际链路检测
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

        /* 1.2、实际链路检测 */
        try {
            loader.testConnection();
        } catch (Exception e) {
            throw new Exception(e);
        }

        /*
         *  2、承接
         *  2.1、老的 dataSource 继续承接新建请求
         *  2.2、新的 dataSource 开始初始化， 完毕后交由 2.3
         *  2.3、新的 dataSource 开始承接新建请求
         *  2.4、老的 dataSource 内部的事务执行完毕， 相继关闭
         *  2.5、老的 dataSource 超过阀值的，强制关闭
         */
        MycatConfig config = MycatServer.getInstance().getConfig();

        /* 2.1 、老的 dataSource 继续承接新建请求， 此处什么也不需要做 */
        boolean isReloadStatusOK = true;

        /* 2.2、新的 dataHosts 初始化 */
        for (PhysicalDBPool dbPool : newDataHosts.values()) {
            String hostName = dbPool.getHostName();
            // 设置 schemas
            ArrayList<String> dnSchemas = new ArrayList<>(30);
            for (PhysicalDBNode dn : newDataNodes.values()) {
                if (dn.getDbPool().getHostName().equals(hostName)) {
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbPool.setSchemas(dnSchemas.toArray(new String[dnSchemas.size()]));

            // 获取 data host
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

        /*
         *  新的 dataHosts 是否初始化成功
         */
        if (isReloadStatusOK) {
            /* 2.3、 在老的配置上，应用新的配置，开始准备承接任务 */
            config.reload(newUsers, newSchemas, newDataNodes, newDataHosts, newErRelations, newFirewall, true);

            /* 2.4、 处理旧的资源 */
            LOGGER.info("1.clear old backend connection(size): " + NIOProcessor.BACKENDS_OLD.size());

            // 清除前一次 reload 转移出去的 old Cons
            Iterator<BackendConnection> iter = NIOProcessor.BACKENDS_OLD.iterator();
            while (iter.hasNext()) {
                BackendConnection con = iter.next();
                con.close("clear old datasources");
                iter.remove();
            }
            Map<String, PhysicalDBPool> oldDataHosts = config.getBackupDataHosts();
            for (PhysicalDBPool dbPool : oldDataHosts.values()) {
                dbPool.stopHeartbeat();

                // 提取数据源下的所有连接
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
            LOGGER.info("2.to be recycled old backend connection(size): " + NIOProcessor.BACKENDS_OLD.size());


            //清理缓存
            MycatServer.getInstance().getCacheService().clearCache();
            if (!loader.isDataHostWithoutWH()) {
                MycatServer.getInstance().reloadMetaData();
            }

            //set the dataHost ready flag
            config.setDataHostWithoutWR(loader.isDataHostWithoutWH());
        } else {
            // 如果重载不成功，则清理已初始化的资源。
            LOGGER.info("reload failed, clear previously created datasources ");
            for (PhysicalDBPool dbPool : newDataHosts.values()) {
                dbPool.clearDataSources("reload config");
                dbPool.stopHeartbeat();
            }
            throw new Exception("Init DbPool failed");
        }
    }

    public static void reload() throws Exception {
        /* 1、载入新的配置， ConfigInitializer 内部完成自检工作 */
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

        /* 2、在老的配置上， 应用新的配置 */
        MycatServer.getInstance().getConfig().reload(users, schemas, dataNodes, dataHosts, erRelations, firewall, false);
        /* 3、清理缓存 */
        MycatServer.getInstance().getCacheService().clearCache();

        if (!loader.isDataHostWithoutWH()) {
            MycatServer.getInstance().reloadMetaData();
        }
    }

}
