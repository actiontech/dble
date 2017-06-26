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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.xmltozk.XmltoZkMain;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.ERTable;
import io.mycat.config.model.FirewallConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;

/**
 * @author mycat
 */
public final class RollbackConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);
	private static String errorMsg;
	public static void execute(ManagerConnection c) {
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
							errorMsg = null;
							if (!rollback()) {
								writeErrorResult(c);
								return;
							}
							XmltoZkMain.rollbackConf();
							//tell zk this instance has prepared
							ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
							//check all session waiting status
							List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
							List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
							// TODO: While waiting, a new instance of MyCat is upping and working.
							while (preparedList.size() < onlineList.size()) {
								LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
								onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
								preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
							}
							for (String child : preparedList) {
								zkConn.delete().forPath(ZKPaths.makePath(KVPathUtil.getConfStatusPath(),child));
							}
							writeOKResult(c);
						} catch (Exception e) {
							LOGGER.warn("reload config failure",e);
							errorMsg = e.toString();
							writeErrorResult(c);
						} finally {
							lock.unlock();
						}
					} finally {
						distributeLock.release();
					}
				}
			} catch (Exception e) {
				LOGGER.warn("reload config failure",e);
				errorMsg = e.toString();
				writeErrorResult(c);
			}
		} else {
			final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
			lock.lock();
			try {
				errorMsg = null;
				if (rollback()) {
					writeOKResult(c);
				} else {
					writeErrorResult(c);
				}
			} finally {
				lock.unlock();
			}
		}
	}
	private static void writeOKResult(ManagerConnection c) {
		StringBuilder s = new StringBuilder();
		s.append(c).append("Rollback config success by manager");
		LOGGER.info(s.toString());
		OkPacket ok = new OkPacket();
		ok.packetId = 1;
		ok.affectedRows = 1;
		ok.serverStatus = 2;
		ok.message = "Rollback config success".getBytes();
		ok.write(c);
	}
	private static void writeErrorResult(ManagerConnection c) {
		String sb = "Rollback config failure.The reason is " + errorMsg;
		LOGGER.warn(sb+"." + String.valueOf(c));
		c.writeErrMessage(ErrorCode.ER_YES, sb);
	}
	public static boolean rollback() {
		MycatConfig conf = MycatServer.getInstance().getConfig();
		Map<String, UserConfig> users = conf.getBackupUsers();
		Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
		Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
		Map<String, PhysicalDBPool> dataHosts = conf.getBackupDataHosts();
		FirewallConfig firewall = conf.getBackupFirewall();
		Map<ERTable, Set<ERTable>> erRelations = conf.getBackupErRelations(); 

		// 检查可回滚状态
		if (!conf.canRollback()) {
			errorMsg ="Conf can not be rollback because of no old version";
			return false;
		}

		Map<String, PhysicalDBPool> cNodes = conf.getDataHosts();
		// 如果回滚已经存在的pool
		boolean rollbackStatus = true;
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.init(dn.getActiveIndex());
			if (!dn.isInitSuccess()) {
				rollbackStatus = false;
				errorMsg ="dataHost"+dn.getHostName()+" inited failure";
				break;
			}
		}
		// 如果回滚不成功，则清理已初始化的资源。
		if (!rollbackStatus) {
			for (PhysicalDBPool dn : dataHosts.values()) {
				dn.clearDataSources("rollbackup config");
				dn.stopHeartbeat();
			}
			return false;
		}
		// 应用回滚
		conf.rollback(users, schemas, dataNodes, dataHosts, erRelations, firewall);

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("clear old config ");
			dn.stopHeartbeat();
		}

		//清理缓存
		 MycatServer.getInstance().getCacheService().clearCache();
		 MycatServer.getInstance().reloadMetaData();
		return true;
	}
}
