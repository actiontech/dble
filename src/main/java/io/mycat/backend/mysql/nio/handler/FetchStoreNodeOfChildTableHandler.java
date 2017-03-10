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
package io.mycat.backend.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.cache.CachePool;
import io.mycat.config.MycatConfig;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.parser.ServerParse;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the datanode to store child table's records
 * 
 * @author wuzhih,huqing.yan
 * 
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(FetchStoreNodeOfChildTableHandler.class);
	private final String sql;
	private AtomicBoolean hadResult = new AtomicBoolean(false);
	private volatile String dataNode;
	private AtomicInteger finished = new AtomicInteger(0);
	protected final ReentrantLock lock = new ReentrantLock();
	private Condition result = lock.newCondition();
	private final NonBlockingSession session;
	public FetchStoreNodeOfChildTableHandler(String sql, NonBlockingSession session) {
		this.sql = sql;
		this.session = session;
	}
	public String execute(String schema, ArrayList<String> dataNodes) {
		String key = schema + ":" + sql;
		CachePool cache = MycatServer.getInstance().getCacheService()
				.getCachePool("ER_SQL2PARENTID");
		String cacheResult = (String) cache.get(key);
		if (cacheResult != null) {
			return cacheResult;
		}
		int totalCount = dataNodes.size();
		MycatConfig conf = MycatServer.getInstance().getConfig();

		LOGGER.debug("find child node with sql:" + sql);
		for (String dn : dataNodes) {
			if (!LOGGER.isDebugEnabled()) {
				//no early return when debug
				if (dataNode != null) {
					LOGGER.debug(" found return ");
					return dataNode;
				}
			}
			PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
			try {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("execute in datanode " + dn);
				}
				RouteResultsetNode node = new RouteResultsetNode(dn, ServerParse.SELECT, sql);
				node.setRunOnSlave(false); // 获取 子表节点，最好走master为好
				BackendConnection conn = session.getTarget(node);
				if (session.tryExistsCon(conn, node)) {
					if (session.closed()) {
						session.clearResources(true);
						return null;
					}
					conn.setResponseHandler(this);
					conn.execute(node, session.getSource(), isAutoCommit());
					
				} else {
					mysqlDN.getConnection(mysqlDN.getDatabase(), true, node, this, node);
				}
			} catch (Exception e) {
				LOGGER.warn("get connection err " + e);
			}
		}
		lock.lock();
		try {
			while (dataNode == null) {
				try {
					result.await(50, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					break;
				}
				if (dataNode != null || finished.get() >= totalCount) {
					break;
				}
			}
		} finally {
			lock.unlock();
		}
		if (!LOGGER.isDebugEnabled()) {
			//no cached when debug
			if (dataNode != null) {
				cache.putIfAbsent(key, dataNode);
			}
		}
		return dataNode;

	}

	private boolean isAutoCommit(){
		return session.getSource().isAutocommit() && !session.getSource().isTxstart();
	}

	private boolean canReleaseConn(){
		if(session.getSource().isClosed()){
			return false;
		}
		return isAutoCommit();
	}
	@Override
	public void connectionAcquired(BackendConnection conn) {
		conn.setResponseHandler(this);
		try {
			conn.query(sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		finished.incrementAndGet();
		LOGGER.warn("connectionError " + e);
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		finished.incrementAndGet();
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		LOGGER.warn("errorResponse " + err.errno + " " + new String(err.message));
		if (canReleaseConn()) {
			conn.release();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("okResponse " + conn);
		}
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			finished.incrementAndGet();
			if (canReleaseConn()) {
				conn.release();
			}
		}
	}

	@Override
	public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received rowResponse response from  " + conn);
		}
		if(hadResult.compareAndSet(false, true)){
			lock.lock();
			try {
				dataNode = ((RouteResultsetNode) conn.getAttachment()).getName();
				result.signal();
			} finally {
				lock.unlock();
			}
		} else {
			LOGGER.warn("find multi data nodes for child table store, sql is:  " + sql);
		}
		return false;
	}


	@Override
	public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("rowEofResponse" + conn);
		}
		finished.incrementAndGet();
		if (canReleaseConn()) {
			conn.release();
		}
	}

	private void executeException(BackendConnection c, Throwable e) {
		finished.incrementAndGet();
		LOGGER.warn("executeException   " + e);
		if (canReleaseConn()) {
			c.release();
		}
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
			boolean isLeft, BackendConnection conn) {
	}
	@Override
	public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
	}

	@Override
	public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
	}
}