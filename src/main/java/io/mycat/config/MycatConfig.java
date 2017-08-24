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
package io.mycat.config;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.config.model.*;
import io.mycat.net.AbstractConnection;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author mycat
 */
public class MycatConfig {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MycatConfig.class);
    private static final int RELOAD = 1;
    private static final int ROLLBACK = 2;
    private static final int RELOAD_ALL = 3;

    private volatile SystemConfig system;
    private volatile FirewallConfig firewall;
    private volatile FirewallConfig firewall2;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, UserConfig> users2;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, SchemaConfig> schemas2;
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBNode> dataNodes2;
    private volatile Map<String, PhysicalDBPool> dataHosts;
    private volatile Map<String, PhysicalDBPool> dataHosts2;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile Map<ERTable, Set<ERTable>> erRelations2;
    private long reloadTime;
    private long rollbackTime;
    private int status;
    private final ReentrantLock lock;

    public boolean isDataHostWithoutWR() {
        return dataHostWithoutWR;
    }

    public void setDataHostWithoutWR(boolean dataHostWithoutWR) {
        this.dataHostWithoutWR = dataHostWithoutWR;
    }

    private volatile boolean dataHostWithoutWR;

    public MycatConfig() {

        //读取schema.xml,rule.xml和server.xml
        ConfigInitializer confInit = new ConfigInitializer(true);
        this.system = confInit.getSystem();
        this.users = confInit.getUsers();
        this.schemas = confInit.getSchemas();
        this.dataHosts = confInit.getDataHosts();
        this.dataNodes = confInit.getDataNodes();
        this.erRelations = confInit.getErRelations();
        this.dataHostWithoutWR = confInit.isDataHostWithoutWH();
        for (PhysicalDBPool dbPool : dataHosts.values()) {
            dbPool.setSchemas(getDataNodeSchemasOfDataHost(dbPool.getHostName()));
        }

        this.firewall = confInit.getFirewall();

        //初始化重加载配置时间
        this.reloadTime = TimeUtil.currentTimeMillis();
        this.rollbackTime = -1L;
        this.status = RELOAD;

        //配置加载锁
        this.lock = new ReentrantLock();
    }

    public SystemConfig getSystem() {
        return system;
    }

    public void setSocketParams(AbstractConnection con, boolean isFrontChannel) throws IOException {
        int sorcvbuf = 0;
        int sosndbuf = 0;
        int soNoDelay = 0;
        if (isFrontChannel) {
            sorcvbuf = system.getFrontSocketSoRcvbuf();
            sosndbuf = system.getFrontSocketSoSndbuf();
            soNoDelay = system.getFrontSocketNoDelay();
        } else {
            sorcvbuf = system.getBackSocketSoRcvbuf();
            sosndbuf = system.getBackSocketSoSndbuf();
            soNoDelay = system.getBackSocketNoDelay();
        }

        NetworkChannel channel = con.getChannel();
        channel.setOption(StandardSocketOptions.SO_RCVBUF, sorcvbuf);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, sosndbuf);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        con.setMaxPacketSize(system.getMaxPacketSize());
        con.setIdleTimeout(system.getIdleTimeout());
        con.setCharset(system.getCharset());
        con.setReadBufferChunk(sorcvbuf);
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public Map<String, UserConfig> getBackupUsers() {
        return users2;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, SchemaConfig> getBackupSchemas() {
        return schemas2;
    }

    public Map<String, PhysicalDBNode> getDataNodes() {
        return dataNodes;
    }

    public String[] getDataNodeSchemasOfDataHost(String dataHost) {
        ArrayList<String> schemas = new ArrayList<>(30);
        for (PhysicalDBNode dn : dataNodes.values()) {
            if (dn.getDbPool().getHostName().equals(dataHost)) {
                schemas.add(dn.getDatabase());
            }
        }
        return schemas.toArray(new String[schemas.size()]);
    }

    public Map<String, PhysicalDBNode> getBackupDataNodes() {
        return dataNodes2;
    }

    public Map<String, PhysicalDBPool> getDataHosts() {
        return dataHosts;
    }

    public Map<String, PhysicalDBPool> getBackupDataHosts() {
        return dataHosts2;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    public Map<ERTable, Set<ERTable>> getBackupErRelations() {
        return erRelations2;
    }

    public FirewallConfig getFirewall() {
        return firewall;
    }

    public FirewallConfig getBackupFirewall() {
        return firewall2;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public long getReloadTime() {
        return reloadTime;
    }

    public long getRollbackTime() {
        return rollbackTime;
    }

    public void reload(Map<String, UserConfig> newUsers, Map<String, SchemaConfig> newSchemas,
                       Map<String, PhysicalDBNode> newDataNodes, Map<String, PhysicalDBPool> newDataHosts,
                       Map<ERTable, Set<ERTable>> newErRelations, FirewallConfig newFirewall,
                       boolean reloadAll) {

        apply(newUsers, newSchemas, newDataNodes, newDataHosts, newErRelations, newFirewall, reloadAll);
        this.reloadTime = TimeUtil.currentTimeMillis();
        this.status = reloadAll ? RELOAD_ALL : RELOAD;
    }

    public boolean canRollback() {
        return users2 != null && schemas2 != null && dataNodes2 != null && dataHosts2 != null &&
                firewall2 != null && status != ROLLBACK;
    }

    public void rollback(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
                         Map<String, PhysicalDBNode> dataNodes, Map<String, PhysicalDBPool> dataHosts,
                         Map<ERTable, Set<ERTable>> erRelations, FirewallConfig firewall) {

        apply(users, schemas, dataNodes, dataHosts, erRelations, firewall, status == RELOAD_ALL);
        this.rollbackTime = TimeUtil.currentTimeMillis();
        this.status = ROLLBACK;
    }

    private DsDiff dsdiff(Map<String, PhysicalDBPool> newDataHosts) {
        DsDiff diff = new DsDiff();
        // deleted datasource
        for (PhysicalDBPool opool : dataHosts.values()) {
            PhysicalDBPool npool = newDataHosts.get(opool.getHostName());
            if (npool == null) {
                LOGGER.warn("reload -delete- failed, use old datasources ");
                return null;
            }

            Map<Integer, PhysicalDatasource[]> odss = opool.getReadSources();
            Map<Integer, PhysicalDatasource[]> ndss = npool.getReadSources();
            Map<Integer, ArrayList<PhysicalDatasource>> idel =
                    new HashMap<>(2);
            boolean haveOne = false;
            for (Map.Entry<Integer, PhysicalDatasource[]> oentry : odss.entrySet()) {
                boolean doadd = false;
                ArrayList<PhysicalDatasource> del = new ArrayList<>();
                for (PhysicalDatasource ods : oentry.getValue()) {
                    boolean dodel = true;
                    for (Map.Entry<Integer, PhysicalDatasource[]> nentry : ndss.entrySet()) {
                        for (PhysicalDatasource nds : nentry.getValue()) {
                            if (ods.getName().equals(nds.getName())) {
                                dodel = false;
                                break;
                            }
                        }
                        if (!dodel) {
                            break;
                        }
                    }
                    if (dodel) {
                        del.add(ods);
                        doadd = true;
                    }
                }
                if (doadd) {
                    idel.put(oentry.getKey(), del);
                    haveOne = true;
                }
            }
            if (haveOne) {
                diff.deled.put(opool, idel);
            }
        }

        // added datasource
        for (PhysicalDBPool npool : newDataHosts.values()) {
            PhysicalDBPool opool = dataHosts.get(npool.getHostName());
            if (opool == null) {
                LOGGER.warn("reload -add- failed, use old datasources ");
                return null;
            }

            Map<Integer, PhysicalDatasource[]> ndss = npool.getReadSources();
            Map<Integer, PhysicalDatasource[]> odss = opool.getReadSources();
            Map<Integer, ArrayList<PhysicalDatasource>> iadd =
                    new HashMap<>(2);
            boolean haveOne = false;
            for (Map.Entry<Integer, PhysicalDatasource[]> nentry : ndss.entrySet()) {
                boolean doadd = false;
                ArrayList<PhysicalDatasource> add = new ArrayList<>();
                for (PhysicalDatasource nds : nentry.getValue()) {
                    boolean isExist = false;
                    for (Map.Entry<Integer, PhysicalDatasource[]> oentry : odss.entrySet()) {
                        for (PhysicalDatasource ods : oentry.getValue()) {
                            if (nds.getName().equals(ods.getName())) {
                                isExist = true;
                                break;
                            }
                        }
                        if (isExist) {
                            break;
                        }
                    }
                    if (!isExist) {
                        add.add(nds);
                        doadd = true;
                    }
                }
                if (doadd) {
                    iadd.put(nentry.getKey(), add);
                    haveOne = true;
                }
            }
            if (haveOne) {
                diff.added.put(opool, iadd);
            }
        }

        return diff;
    }

    private void apply(Map<String, UserConfig> newUsers,
                       Map<String, SchemaConfig> newSchemas,
                       Map<String, PhysicalDBNode> newDataNodes,
                       Map<String, PhysicalDBPool> newDataHosts,
                       Map<ERTable, Set<ERTable>> newErRelations,
                       FirewallConfig newFirewall,
                       boolean isLoadAll) {
        final ReentrantReadWriteLock lock = MycatServer.getInstance().getConfLock();
        lock.writeLock().lock();
        try {
            // old 处理
            // 1、停止老的数据源心跳
            // 2、备份老的数据源配置
            //--------------------------------------------
            if (isLoadAll) {
                Map<String, PhysicalDBPool> oldDataHosts = this.dataHosts;
                if (oldDataHosts != null) {
                    for (PhysicalDBPool oldDbPool : oldDataHosts.values()) {
                        if (oldDbPool != null) {
                            oldDbPool.stopHeartbeat();
                        }
                    }
                }
                this.dataNodes2 = this.dataNodes;
                this.dataHosts2 = this.dataHosts;
            }

            this.users2 = this.users;
            this.schemas2 = this.schemas;
            this.firewall2 = this.firewall;
            this.erRelations2 = this.erRelations;
//            comment BY huqing.yan and will reopen later
//            if (!isLoadAll) {
//                DsDiff diff = dsdiff(newDataHosts);
//                diff.apply();
//            }
            // new 处理
            // 1、启动新的数据源心跳
            // 2、执行新的配置
            //---------------------------------------------------
            if (isLoadAll) {
                if (newDataHosts != null) {
                    for (PhysicalDBPool newDbPool : newDataHosts.values()) {
                        if (newDbPool != null) {
                            MycatServer.getInstance().saveDataHostIndex(newDbPool.getHostName(), newDbPool.getActiveIndex());
                            newDbPool.startHeartbeat();
                        }
                    }
                }
                this.dataNodes = newDataNodes;
                this.dataHosts = newDataHosts;
            }
            this.users = newUsers;
            this.schemas = newSchemas;
            this.firewall = newFirewall;
            this.erRelations = newErRelations;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class DsDiff {
        public Map<PhysicalDBPool, Map<Integer, ArrayList<PhysicalDatasource>>> deled;
        public Map<PhysicalDBPool, Map<Integer, ArrayList<PhysicalDatasource>>> added;

        DsDiff() {
            deled = new HashMap<>(2);
            added = new HashMap<>(2);
        }

        public void apply() {
            // delete
            for (Map.Entry<PhysicalDBPool, Map<Integer, ArrayList<PhysicalDatasource>>> lentry : deled.entrySet()) {
                for (Map.Entry<Integer, ArrayList<PhysicalDatasource>> llentry : lentry.getValue().entrySet()) {
                    for (int i = 0; i < llentry.getValue().size(); i++) {
                        // lentry.getKey().delRDs(llentry.getValue().get(i));
                        llentry.getValue().get(i).setDying();
                    }
                }
            }

            // add
            for (Map.Entry<PhysicalDBPool, Map<Integer, ArrayList<PhysicalDatasource>>> lentry : added.entrySet()) {
                for (Map.Entry<Integer, ArrayList<PhysicalDatasource>> llentry : lentry.getValue().entrySet()) {
                    for (int i = 0; i < llentry.getValue().size(); i++) {
                        lentry.getKey().addRDs(llentry.getKey(), llentry.getValue().get(i));
                    }
                }
            }

            // sleep
            ArrayList<PhysicalDatasource> killed = new ArrayList<>(2);
            for (Map.Entry<PhysicalDBPool, Map<Integer, ArrayList<PhysicalDatasource>>> lentry : deled.entrySet()) {
                for (Map.Entry<Integer, ArrayList<PhysicalDatasource>> llentry : lentry.getValue().entrySet()) {
                    for (int i = 0; i < llentry.getValue().size(); i++) {
                        if (llentry.getValue().get(i).getActiveCount() != 0) {
                            killed.add(llentry.getValue().get(i));
                        }
                    }
                }
            }
            if (!killed.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                    //ignore error
                }

                for (PhysicalDatasource aKilled : killed) {
                    if (aKilled.getActiveCount() != 0) {
                        aKilled.clearConsByDying();
                    }
                }
            }
        }
    }
}


