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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.LoadDataUtil;
import io.mycat.backend.mysql.nio.handler.transaction.AutoTxOperation;
import io.mycat.backend.mysql.nio.handler.transaction.normal.NormalAutoCommitNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.normal.NormalAutoRollbackNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.xa.XAAutoCommitNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.xa.XAAutoRollbackNodesHandler;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.net.mysql.BinaryRowDataPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.AbstractDataNodeMerge;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.DataMergeService;
import io.mycat.sqlengine.mpp.DataNodeMergeManager;
import io.mycat.sqlengine.mpp.MergeCol;
import io.mycat.statistic.stat.QueryResult;
import io.mycat.statistic.stat.QueryResultDispatcher;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

	private final RouteResultset rrs;
	private final NonBlockingSession session;
	private final AbstractDataNodeMerge dataMergeSvr;
	private final boolean sessionAutocommit;
	private String priamaryKeyTable = null;
	private int primaryKeyIndex = -1;
	private int fieldCount = 0;
	private final ReentrantLock lock;
	private long affectedRows;
	private long selectRows;
	private long insertId;
	private volatile boolean fieldsReturned;
	private final boolean isCallProcedure;
	private long startTime;
	private long netInBytes;
	private long netOutBytes;
	protected volatile boolean terminated;
	private boolean prepared;
	private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();
	private int isOffHeapuseOffHeapForMerge = 1;
	private ErrorPacket err;
	private List<BackendConnection> errConnection;

	public MultiNodeQueryHandler(int sqlType, RouteResultset rrs,
			 NonBlockingSession session) {
		
		super(session);
		
		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("execute mutinode query " + rrs.getStatement());
		}
		
		this.rrs = rrs;
		isOffHeapuseOffHeapForMerge = MycatServer.getInstance().
				getConfig().getSystem().getUseOffHeapForMerge();
		if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
			/**
			 * 使用Off Heap
			 */
			if(isOffHeapuseOffHeapForMerge == 1){
				dataMergeSvr = new DataNodeMergeManager(this,rrs);
			}else {
				dataMergeSvr = new DataMergeService(this,rrs);
			}
		} else {
			dataMergeSvr = null;
		}
		
		isCallProcedure = rrs.isCallStatement();
		this.sessionAutocommit = session.getSource().isAutocommit();
		this.session = session;
		this.lock = new ReentrantLock();
		if ((dataMergeSvr != null)
				&& LOGGER.isDebugEnabled()) {
				LOGGER.debug("has data merge logic ");
		}
	}

	protected void reset(int initCount) {
		super.reset(initCount);
		this.netInBytes = 0;
		this.netOutBytes = 0;
		this.terminated = false;
	}

	public NonBlockingSession getSession() {
		return session;
	}

	public void execute() throws Exception {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			this.reset(rrs.getNodes().length);
			this.fieldsReturned = false;
			this.affectedRows = 0L;
			this.insertId = 0L;
		} finally {
			lock.unlock();
		}
		MycatConfig conf = MycatServer.getInstance().getConfig();
		startTime = System.currentTimeMillis();
		LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
		StringBuilder sb = new StringBuilder();
		for (final RouteResultsetNode node : rrs.getNodes()) {
			if(node.isModifySQL()){
				sb.append("["+node.getName()+"]"+node.getStatement()).append(";\n");
			}
		}
		if(sb.length()>0){
			TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
		}
		for (final RouteResultsetNode node : rrs.getNodes()) {
			BackendConnection conn = session.getTarget(node);
			if (session.tryExistsCon(conn, node)) {
				LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
				node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
				LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
				_execute(conn, node);
			} else {
				// create new connection
				LOGGER.debug("node.getRunOnSlave()1-" + node.getRunOnSlave());
				node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
				LOGGER.debug("node.getRunOnSlave()2-" + node.getRunOnSlave());
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), sessionAutocommit, node, this, node);
				// 注意该方法不仅仅是获取连接，获取新连接成功之后，会通过层层回调，最后回调到本类 的connectionAcquired
				// 这是通过 上面方法的 this 参数的层层传递完成的。
				// connectionAcquired 进行执行操作:
				// session.bindConnection(node, conn);
				// _execute(conn, node);
			}
		}
	}

	private void _execute(BackendConnection conn, RouteResultsetNode node) {
		if (clearIfSessionClosed(session)) {
			return;
		}
		conn.setResponseHandler(this);
		conn.execute(node, session.getSource(), sessionAutocommit&&!session.getSource().isTxstart()&&!node.isModifySQL());
	}
	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		LOGGER.warn("backend connect"+reason);
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.packetId = ++packetId;
		errPacket.errno = ErrorCode.ER_ABORTING_CONNECTION;
		errPacket.message =  StringUtil.encode(reason, session.getSource().getCharset());
		err = errPacket;
		lock.lock();
		try {
			if (!terminated) {
				terminated = true;
			}
			if (errConnection == null) {
				errConnection = new ArrayList<BackendConnection>();
			}
			errConnection.add(conn);
			if (--nodeCount <= 0) {
				session.handleSpecial(rrs, session.getSource().getSchema(), false);
				handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
			}
		} finally {
			lock.unlock();
		}
	}
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.warn("backend connect", e);
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.packetId = ++packetId;
		errPacket.errno = ErrorCode.ER_ABORTING_CONNECTION;
		errPacket.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());
		err = errPacket;
		lock.lock();
		try {
			if (!terminated) {
				terminated = true;
			}
			if (errConnection == null) {
				errConnection = new ArrayList<BackendConnection>();
			}
			errConnection.add(conn);
			if (--nodeCount <= 0) {
				session.handleSpecial(rrs, session.getSource().getSchema(), false);
				handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		final RouteResultsetNode node = (RouteResultsetNode) conn
				.getAttachment();
		session.bindConnection(node, conn);
		_execute(conn, node);
	}


	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(data);
		errPacket.packetId = 1;//TODO :CONFIRM ?++packetId??
		err = errPacket;
		lock.lock();
		try {
			if (!isFail())
				setFail(err.toString());
			if (--nodeCount > 0)
				return;
			session.handleSpecial(rrs, session.getSource().getSchema(), false);
			handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		this.netOutBytes += data.length;
		boolean executeResponse = conn.syncAndExcute();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received ok response ,executeResponse:"
					+ executeResponse + " from " + conn);
		}
		if (executeResponse) {
			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
			lock.lock();
			try {
				// 判断是否是全局表，如果是，执行行数不做累加，以最后一次执行的为准。
				if (!rrs.isGlobalTable()) {
					affectedRows += ok.affectedRows;
				} else {
					affectedRows = ok.affectedRows;
				}
				if (ok.insertId > 0) {
					insertId = (insertId == 0) ? ok.insertId : Math.min(
							insertId, ok.insertId);
				}
				if (--nodeCount > 0)
					return;
				if(isFail()||terminated){
					session.handleSpecial(rrs, source.getSchema(), false);
					handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
					return;
				}
				session.handleSpecial(rrs, source.getSchema(), true);
				if (rrs.isLoadData()) {
					byte lastPackId = source.getLoadDataInfileHandler()
							.getLastPackId();
					ok.packetId = ++lastPackId;// OK_PACKET
					ok.message = ("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0")
							.getBytes();// 此处信息只是为了控制台给人看的
					source.getLoadDataInfileHandler().clear();
				} else {
					ok.packetId = ++packetId;// OK_PACKET
				}

				ok.affectedRows = affectedRows;
				ok.serverStatus = source.isAutocommit() ? 2 : 1;
				if (insertId > 0) {
					ok.insertId = insertId;
					source.setLastInsertId(insertId);
				}
				handleEndPacket(ok.toBytes(), AutoTxOperation.COMMIT, conn); 
			}finally {
				lock.unlock();
			}
		}
	}

	@Override
	public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("on row end reseponse " + conn);
		}
		
		this.netOutBytes += eof.length;
		
		if (errorRepsponsed.get()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			// conn.close(this.error);
			return;
		}

		final ServerConnection source = session.getSource();
		if (!isCallProcedure) {
			if (clearIfSessionClosed(session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
		}

		if (decrementCountBy(1)) {
            if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue())) {
				if (this.sessionAutocommit && !session.getSource().isTxstart() && !session.getSource().isLocked()) {// clear all connections
					session.releaseConnections(false);
				}

				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
			}
			if (dataMergeSvr != null) {
				try {
					dataMergeSvr.outputMergeResult(session, eof);
				} catch (Exception e) {
					handleDataProcessException(e);
				}

			} else {
				try {
					lock.lock();
					eof[3] = ++packetId;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("last packet id:" + packetId);
					}
					source.write(eof);
				} finally {
					lock.unlock();

				}
			}
			if (MycatServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
				int resultSize = source.getWriteQueue().size() * MycatServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
				if (rrs != null && rrs.getStatement() != null) {
					netInBytes += rrs.getStatement().getBytes().length;
				}
				// 查询结果派发
				QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(),
						rrs.getStatement(), selectRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),resultSize);
				QueryResultDispatcher.dispatchQuery(queryResult);
			}
		}

	}

	/**
	 * 将汇聚结果集数据真正的发送给Mycat客户端
	 * @param source
	 * @param eof
	 * @param
	 */
	public void outputMergeResult(final ServerConnection source, final byte[] eof, Iterator<UnsafeRow> iter) {
		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			/**
			 * 处理limit语句的start 和 end位置，将正确的结果发送给
			 * Mycat 客户端
			 */
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();
			int index = 0;

			if (start < 0)
				start = 0;

			if (rrs.getLimitSize() < 0)
				end = Integer.MAX_VALUE;

			if(prepared) {
				while (iter.hasNext()){
					UnsafeRow row = iter.next();
					if(index >= start){
						row.packetId = ++packetId;
						BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
						binRowPacket.read(fieldPackets, row);
						buffer = binRowPacket.write(buffer, source, true);
					}
					index++;
					if(index == end){
						break;
					}
				}
			} else {
				while (iter.hasNext()){
					UnsafeRow row = iter.next();
					if(index >= start){
						row.packetId = ++packetId;
						buffer = row.write(buffer,source,true);
					}
					index++;
					if(index == end){
						break;
					}
				}
			}
			
			eof[3] = ++packetId;

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}

			/**
			 * 真正的开始把Writer Buffer的数据写入到channel 中
			 */
			source.write(source.writeToBuffer(eof, buffer));
		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			dataMergeSvr.clear();
			lock.unlock();
		}
	}
	public void outputMergeResult(final ServerConnection source,
			final byte[] eof, List<RowDataPacket> results) {
		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			// 处理limit语句
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();

			if (start < 0) {
				start = 0;
			}

			if (rrs.getLimitSize() < 0) {
				end = results.size();
			}
				
//			// 对于不需要排序的语句,返回的数据只有rrs.getLimitSize()
//			if (rrs.getOrderByCols() == null) {
//				end = results.size();
//				start = 0;
//			}
			if (end > results.size()) {
				end = results.size();
			}
			
//			for (int i = start; i < end; i++) {
//				RowDataPacket row = results.get(i);
//				if( prepared ) {
//					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
//					binRowDataPk.read(fieldPackets, row);
//					binRowDataPk.packetId = ++packetId;
//					//binRowDataPk.write(source);
//					buffer = binRowDataPk.write(buffer, session.getSource(), true);
//				} else {
//					row.packetId = ++packetId;
//					buffer = row.write(buffer, source, true);
//				}
//			}
			
			if(prepared) {
				for (int i = start; i < end; i++) {
					RowDataPacket row = results.get(i);
					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
					binRowDataPk.read(fieldPackets, row);
					binRowDataPk.packetId = ++packetId;
					//binRowDataPk.write(source);
					buffer = binRowDataPk.write(buffer, session.getSource(), true);
				}
			} else {
				for (int i = start; i < end; i++) {
					RowDataPacket row = results.get(i);
					row.packetId = ++packetId;
					buffer = row.write(buffer, source, true);
				}
			}

			eof[3] = ++packetId;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}
			source.write(source.writeToBuffer(eof, buffer));

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			dataMergeSvr.clear();
			lock.unlock();
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsnull, byte[] eof,
			boolean isLeft, BackendConnection conn) {
		
		
		this.netOutBytes += header.length;
		this.netOutBytes += eof.length;
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			this.netOutBytes += field.length;
		}
		
		ServerConnection source = null;

		if (fieldsReturned) {
			return;
		}
		lock.lock();
		try {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;

			boolean needMerg = (dataMergeSvr != null)
					&& dataMergeSvr.getRrs().needMerge();
			Set<String> shouldRemoveAvgField = new HashSet<>();
			Set<String> shouldRenameAvgField = new HashSet<>();
			if (needMerg) {
				Map<String, Integer> mergeColsMap = dataMergeSvr.getRrs()
						.getMergeCols();
				if (mergeColsMap != null) {
					for (Map.Entry<String, Integer> entry : mergeColsMap
							.entrySet()) {
						String key = entry.getKey();
						int mergeType = entry.getValue();
						if (MergeCol.MERGE_AVG == mergeType
								&& mergeColsMap.containsKey(key + "SUM")) {
							shouldRemoveAvgField.add((key + "COUNT")
									.toUpperCase());
							shouldRenameAvgField.add((key + "SUM")
									.toUpperCase());
						}
					}
				}

			}

			source = session.getSource();
			ByteBuffer buffer = source.allocate();
			fieldCount = fields.size();
			if (shouldRemoveAvgField.size() > 0) {
				ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
				packet.packetId = ++packetId;
				packet.fieldCount = fieldCount - shouldRemoveAvgField.size();
				buffer = packet.write(buffer, source, true);
			} else {

				header[3] = ++packetId;
				buffer = source.writeToBuffer(header, buffer);
			}

			String primaryKey = null;
			if (rrs.hasPrimaryKeyToCache()) {
				String[] items = rrs.getPrimaryKeyItems();
				priamaryKeyTable = items[0];
				primaryKey = items[1];
			}

			Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
					fieldCount);

			for (int i = 0, len = fieldCount; i < len; ++i) {
				boolean shouldSkip = false;
				byte[] field = fields.get(i);
				if (needMerg) {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					fieldPackets.add(fieldPkg);
					String fieldName = new String(fieldPkg.name).toUpperCase();
					if (columToIndx != null
							&& !columToIndx.containsKey(fieldName)) {
						if (shouldRemoveAvgField.contains(fieldName)) {
							shouldSkip = true;
							fieldPackets.remove(fieldPackets.size() - 1);
						}
						if (shouldRenameAvgField.contains(fieldName)) {
							String newFieldName = fieldName.substring(0,
									fieldName.length() - 3);
							fieldPkg.name = newFieldName.getBytes();
							fieldPkg.packetId = ++packetId;
							shouldSkip = true;
							// 处理AVG字段位数和精度, AVG位数 = SUM位数 - 14
							fieldPkg.length = fieldPkg.length - 14;
							// AVG精度 = SUM精度 + 4
 							fieldPkg.decimals = (byte) (fieldPkg.decimals + 4);
							buffer = fieldPkg.write(buffer, source, false);

							// 还原精度
							fieldPkg.decimals = (byte) (fieldPkg.decimals - 4);
						}

						ColMeta colMeta = new ColMeta(i, fieldPkg.type);
						colMeta.decimals = fieldPkg.decimals;
						columToIndx.put(fieldName, colMeta);
					}
				} else {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					fieldPackets.add(fieldPkg);
					fieldCount = fields.size();
					if (primaryKey != null && primaryKeyIndex == -1) {
					// find primary key index
					String fieldName = new String(fieldPkg.name);
					if (primaryKey.equalsIgnoreCase(fieldName)) {
						primaryKeyIndex = i;
					}
				}   }
				if (!shouldSkip) {
					field[3] = ++packetId;
					buffer = source.writeToBuffer(field, buffer);
				}
			}
			eof[3] = ++packetId;
			buffer = source.writeToBuffer(eof, buffer);
			source.write(buffer);
			if (dataMergeSvr != null) {
				dataMergeSvr.onRowMetaData(columToIndx, fieldCount);

			}
		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
	}

	public void handleDataProcessException(Exception e) {
		if (!errorRepsponsed.get()) {
			this.error = e.toString();
			LOGGER.warn("caught exception ", e);
			setFail(e.toString());
			this.tryErrorFinished(true);
		}
	}

	@Override
	public boolean rowResponse(final byte[] row, RowDataPacket rowPacketnull, boolean isLeft, BackendConnection conn) {
		
		if (errorRepsponsed.get()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			//conn.close(error);
			return true;
		}
		
		
		lock.lock();
		try {
			
			this.selectRows++;
			
			RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
			String dataNode = rNode.getName();
			if (dataMergeSvr != null) {
				// even through discarding the all rest data, we can't
				//close the connection for tx control such as rollback or commit.
				// So the "isClosedByDiscard" variable is unnecessary.
				// @author Uncle-pan
				// @since 2016-03-25
				dataMergeSvr.onNewRecord(dataNode, row);
			} else {
				RowDataPacket rowDataPkg =null;
				// cache primaryKey-> dataNode
				if (primaryKeyIndex != -1) {
					 rowDataPkg = new RowDataPacket(fieldCount);
					rowDataPkg.read(row);
					String primaryKey = new String(rowDataPkg.fieldValues.get(primaryKeyIndex));
					LayerCachePool pool = MycatServer.getInstance().getRouterservice().getTableId2DataNodeCache();
					pool.putIfAbsent(priamaryKeyTable, primaryKey, dataNode);
				}
				row[3] = ++packetId;
				if( prepared ) {
					if(rowDataPkg==null) {
						rowDataPkg = new RowDataPacket(fieldCount);
						rowDataPkg.read(row);
					}
					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
					binRowDataPk.read(fieldPackets, rowDataPkg);
					binRowDataPk.write(session.getSource());
				} else {
					session.getSource().write(row);
				}
			}

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public void clearResources() {
		lock.lock();
		try {
			if (dataMergeSvr != null) {
				dataMergeSvr.clear();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void writeQueueAvailable() {
	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}
	
	public boolean isPrepared() {
		return prepared;
	}

	public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}
	protected void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn) {
		ServerConnection source = session.getSource();
		if (source.isAutocommit() && !source.isTxstart() && conn.isModifiedSQLExecuted()) {
			if (nodeCount < 0) {
				return;
			}
			//隐式分布式事务，自动发起commit or rollback
			if (txOperation == AutoTxOperation.COMMIT) {
				if (session.getXaState() == null) {
					NormalAutoCommitNodesHandler autoHandler = new NormalAutoCommitNodesHandler(session, data);
					autoHandler.commit();
				} else {
					XAAutoCommitNodesHandler autoHandler = new XAAutoCommitNodesHandler(session, data, rrs.getNodes());
					autoHandler.commit();
				}
			} else {
				if (session.getXaState() == null) {
					NormalAutoRollbackNodesHandler autoHandler = new NormalAutoRollbackNodesHandler(session, data, rrs.getNodes(), errConnection);
					autoHandler.rollback();
				} else {
					XAAutoRollbackNodesHandler autoHandler = new XAAutoRollbackNodesHandler(session, data, rrs.getNodes(), errConnection);
					autoHandler.rollback();
				}
			}
		} else {
			boolean inTransaction = !source.isAutocommit() || source.isTxstart();
			if (!inTransaction) {
				// 普通查询
				session.releaseConnection(conn);
			}
			// 显示分布式事务
			if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
				source.setTxInterrupt("ROLLBACK");
			}
			if (nodeCount == 0) {
				session.getSource().write(data);
			}
		}
	}
}
