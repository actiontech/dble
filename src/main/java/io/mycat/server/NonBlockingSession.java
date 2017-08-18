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
package io.mycat.server;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.KillConnectionHandler;
import io.mycat.backend.mysql.nio.handler.LockTablesHandler;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.backend.mysql.nio.handler.MultiNodeDdlHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.backend.mysql.nio.handler.SingleNodeHandler;
import io.mycat.backend.mysql.nio.handler.UnLockTablesHandler;
import io.mycat.backend.mysql.nio.handler.builder.HandlerBuilder;
import io.mycat.backend.mysql.nio.handler.query.impl.OutputHandler;
import io.mycat.backend.mysql.nio.handler.transaction.CommitNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.RollbackNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.normal.NormalCommitNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.normal.NormalRollbackNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.xa.XACommitNodesHandler;
import io.mycat.backend.mysql.nio.handler.transaction.xa.XARollbackNodesHandler;
import io.mycat.backend.mysql.store.memalloc.MemSizeController;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.optimizer.MyOptimizer;
import io.mycat.plan.visitor.MySQLPlanNodeVisitor;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.parser.ServerParse;

/**
 * @author mycat
 */
public class NonBlockingSession implements Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);
	public static final int CANCEL_STATUS_INIT = 0;
	public static final int CANCEL_STATUS_COMMITING = 1;
	public static final int CANCEL_STATUS_CANCELING = 2;


    private final ServerConnection source;
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;
    // life-cycle: each sql execution
    private volatile SingleNodeHandler singleNodeHandler;
    private volatile MultiNodeQueryHandler multiNodeHandler;
    private volatile MultiNodeDdlHandler multiNodeDdlHandler;
    private RollbackNodesHandler rollbackHandler;
    private CommitNodesHandler commitHandler;
    private volatile String xaTXID;
    private volatile TxState xaState;
	private boolean prepared;
	private volatile boolean needWaitFinished = false;
	// 取消状态 0 - 初始 1 - 提交进行  2 - 切断进行
	private int cancelStatus = 0;

	private ResponseHandler responseHandler;
	private OutputHandler outputHandler;
	private volatile boolean terminated;
	private volatile boolean inTransactionKilled;
	
	// 以链接为单位，对链接中使用的join，orderby以及其它内存使用进行控制
	private MemSizeController joinBufferMC;
	private MemSizeController orderBufferMC;
	private MemSizeController otherBufferMC;


	public NonBlockingSession(ServerConnection source) {
		this.source = source;
		this.target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>(2, 1f);
		this.joinBufferMC = new MemSizeController(4 * 1024 * 1024);
		this.orderBufferMC = new MemSizeController(4 * 1024 * 1024);
		this.otherBufferMC = new MemSizeController(4 * 1024 * 1024);
	}
    public OutputHandler getOutputHandler() {
		return outputHandler;
	}

	public void setOutputHandler(OutputHandler outputHandler) {
		this.outputHandler = outputHandler;
	}
    @Override
    public ServerConnection getSource() {
        return source;
    }

    @Override
    public int getTargetCount() {
        return target.size();
    }

    public Set<RouteResultsetNode> getTargetKeys() {
        return target.keySet();
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
        return this.target;
    }

    public TxState getXaState() {
		return xaState;
	}

	public void setXaState(TxState xaState) {
		this.xaState = xaState;
	}

    public boolean isNeedWaitFinished() {
		return needWaitFinished;
	}

	/**
	 * 获取并验证锁的方法
	 */
	public synchronized boolean cancelableStatusSet(int value){
		//这个锁其实只有在1和2冲突的时候才会有提示
		if( (value|this.cancelStatus) > 2 ){
			return false;
		}
		this.cancelStatus = value;
		return true;
	}

    @Override
    public void execute(RouteResultset rrs, int type) {
        // clear prev execute resources
        clearHandlesResources();
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            LOGGER.debug(s.append(source).append(rrs).toString() + " rrs ");
        }
        // 检查路由结果是否为空
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
			if (rrs.isNeedOptimizer()) {
				executeMultiSelect(rrs);
			} else {
				source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
						"No dataNode found ,please check tables defined in schema:" + source.getSchema());
			}
			return;
        }
		if (this.getSessionXaID() != null && this.xaState == TxState.TX_INITIALIZE_STATE) {
			this.xaState = TxState.TX_STARTED_STATE;
		}
		if (nodes.length == 1) {
			singleNodeHandler = new SingleNodeHandler(rrs, this);
			if (this.isPrepared()) {
				singleNodeHandler.setPrepared(true);
			}
			try {
				singleNodeHandler.execute();
			} catch (Exception e) {
				handleSpecial(rrs, source.getSchema(), false);
				LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
				source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
			if (this.isPrepared()) {
				this.setPrepared(false);
			}
		} else {
			if (rrs.getSqlType() != ServerParse.DDL) {
				/**
				 * here, just a try! The sync is the superfluous, because there are hearbeats at every backend node.
				 * We don't do 2pc or 3pc. Beause mysql(that is, resource manager) don't support that for ddl statements.
				 */
				multiNodeHandler = new MultiNodeQueryHandler(type, rrs, this);
				if (this.isPrepared()) {
					multiNodeHandler.setPrepared(true);
				}
				try {
					multiNodeHandler.execute();
				} catch (Exception e) {
					handleSpecial(rrs, source.getSchema(), false);
					LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
					source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
				}
				if (this.isPrepared()) {
					this.setPrepared(false);
				}
			} else {
				checkBackupStatus();
				multiNodeDdlHandler = new MultiNodeDdlHandler(type, rrs, this);
				try {
					multiNodeDdlHandler.execute();
				} catch (Exception e) {
					LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
					source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
				}
			}
		}
    }

    private void executeMultiSelect(RouteResultset rrs){
//    	if (this.source.isTxInterrupted()) {
//			sendErrorPacket(ErrorCode.ER_YES, "Transaction error, need to rollback.");
//			return;
//		}
    	SQLSelectStatement ast = (SQLSelectStatement)rrs.getSqlStatement();
		MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(this.getSource().getSchema(),this.getSource().getCharsetIndex());
		visitor.visit(ast);
		PlanNode node = visitor.getTableNode();
		node.setSql(rrs.getStatement());
		node.setUpFields();
		checkTablesPrivilege(node,ast);
		node = MyOptimizer.optimize(node);
//		if (LOGGER.isInfoEnabled()) {
//			long currentTime = System.nanoTime();
//			StringBuilder builder = new StringBuilder();
//			builder.append(toString()).append("| sql optimize's elapsedTime is ")
//					.append(currentTime - getExecutedNanos());
//			logger.info(builder.toString());
//			setExecutedNanos(currentTime);
//		}
		execute(node);
    }
    private void checkTablesPrivilege(PlanNode node, SQLSelectStatement stmt){
    	for (TableNode tn : node.getReferedTableNodes()) {
    		if (!MycatPrivileges.checkPrivilege(source, tn.getSchema(), tn.getTableName(), Checktype.SELECT)) {
				String msg = "The statement DML privilege check is not passed, sql:" + stmt;
				throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "",msg );
			}
    	}
    }
    
    public void execute(PlanNode node) {
		init();
		HandlerBuilder builder = new HandlerBuilder(node, this);
		try {
			builder.build(false);//no next
		} catch (SQLSyntaxErrorException e) {
			LOGGER.warn(new StringBuilder().append(source).append(" execute plan is : ").append(node).toString(), e);
//			source.setCurrentSQL(null);
			source.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
		} catch (NoSuchElementException e) {
			LOGGER.warn(new StringBuilder().append(source).append(" execute plan is : ").append(node).toString(), e);
//			source.setCurrentSQL(null);
			this.terminate();
			source.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
		} catch (Exception e) {
			LOGGER.warn(new StringBuilder().append(source).append(" execute plan is : ").append(node).toString(), e);
//			source.setCurrentSQL(null);
			this.terminate();
			source.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
		}
	}
    
    private void init() {
		this.outputHandler = null;
		this.responseHandler = null;
		this.terminated = false;
		if (inTransactionKilled) {
			//TODO:YHQ
			// kill query is asynchronized, wait for last query is killed.
//			for (BackendConnection conn : target.values()) {
//				while (conn.isRunning()) {
//					LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
//				}
//			}
			inTransactionKilled = false;
		}
	}
    public void onQueryError(byte[] message) {
//		source.unlockTable();
//		source.getIsRunning().set(false);
		if (outputHandler != null)
			outputHandler.backendConnError(message);
	}
    private CommitNodesHandler createCommitNodesHandler() {
		if (commitHandler == null) {
			if (this.getSessionXaID() == null) {
				commitHandler = new NormalCommitNodesHandler(this);
			} else {
				commitHandler = new XACommitNodesHandler(this);
			}
		} else {
			if (this.getSessionXaID() == null && (commitHandler instanceof XACommitNodesHandler)) {
				commitHandler = new NormalCommitNodesHandler(this);
			}
			if (this.getSessionXaID() != null && (commitHandler instanceof NormalCommitNodesHandler)) {
				commitHandler = new XACommitNodesHandler(this);
			}
		}
		return commitHandler;
	}

	public void commit() {
		final int initCount = target.size();
		if (initCount <= 0) {
			clearResources(false);
			ByteBuffer buffer = source.allocate();
			buffer = source.writeToBuffer(OkPacket.OK, buffer);
			source.write(buffer);
			return;
		}
		checkBackupStatus();
		createCommitNodesHandler();
		commitHandler.commit();
	}

	public void checkBackupStatus() {
		while (MycatServer.getInstance().isBackupLocked()) {
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
		}
		needWaitFinished = true;
	}
	private RollbackNodesHandler createRollbackNodesHandler() {
		if (rollbackHandler == null) {
			if (this.getSessionXaID() == null) {
				rollbackHandler = new NormalRollbackNodesHandler(this);
			} else {
				rollbackHandler = new XARollbackNodesHandler(this);
			}
		} else {
			if (this.getSessionXaID() == null && (rollbackHandler instanceof XARollbackNodesHandler)) {
				rollbackHandler = new NormalRollbackNodesHandler(this);
			}
			if (this.getSessionXaID() != null && (rollbackHandler instanceof NormalRollbackNodesHandler)) {
				rollbackHandler = new XARollbackNodesHandler(this);
			}
		}
		return rollbackHandler;
	}

	public void rollback() {
		final int initCount = target.size();
		if (initCount <= 0) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("no session bound connections found ,no need send rollback cmd ");
			}
			clearResources(false);
			ByteBuffer buffer = source.allocate();
			buffer = source.writeToBuffer(OkPacket.OK, buffer);
			source.write(buffer);
			return;
		}
		createRollbackNodesHandler();
		rollbackHandler.rollback();
	}

	/**
	 * 执行lock tables语句方法
	 * @author songdabin
	 * @date 2016-7-9
	 * @param rrs
	 */
	public void lockTable(RouteResultset rrs) {
		// 检查路由结果是否为空
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ source.getSchema());
			return;
		}
		LockTablesHandler handler = new LockTablesHandler(this, rrs);
		source.setLocked(true);
		try {
			handler.execute();
		} catch (Exception e) {
			LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
			source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
		}
	}

	/**
	 * 执行unlock tables语句方法
	 * @author songdabin
	 * @date 2016-7-9
	 * @param sql
	 */
	public void unLockTable(String sql) {
		UnLockTablesHandler handler = new UnLockTablesHandler(this, this.source.isAutocommit(), sql);
		handler.execute();
	}
	
    @Override
    public void cancel(FrontendConnection sponsor) {

    }

    /**
     * {@link ServerConnection#isClosed()} must be true before invoking this
     */
    public void terminate() {
        closeAndClearResources("client closed ");
    }

	/**
	 * Only used when kill @@connection is Issued
	 */
	public void initiativeTerminate(){

		for (BackendConnection node : target.values()) {
			node.terminate("client closed ");
		}
		target.clear();
		clearHandlesResources();
	}

    public void closeAndClearResources(String reason) {
		// XA MUST BE FINISHED
		if (source.isTxstart() && this.getXaState() != null && this.getXaState() != TxState.TX_INITIALIZE_STATE) {
			return;
		}
        for (BackendConnection node : target.values()) {
            node.terminate(reason);
        }
        target.clear();
        clearHandlesResources();
    }

    public void releaseConnectionIfSafe(BackendConnection conn, boolean debug, boolean needRollBack) {
        RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        if (node != null) {
            if ((this.source.isAutocommit() || conn.isFromSlaveDB())&&!this.source.isTxstart() && !this.source.isLocked()) {
                releaseConnection((RouteResultsetNode) conn.getAttachment(), LOGGER.isDebugEnabled(), needRollBack);
            }
        }
    }

    public void releaseConnection(RouteResultsetNode rrn, boolean debug, final boolean needRollback) {

        BackendConnection c = target.remove(rrn);
        if (c != null) {
            if (debug) {
                LOGGER.debug("release connection " + c);
            }
            if (c.getAttachment() != null) {
                c.setAttachment(null);
            }
			if (!c.isClosedOrQuit()) {
				if (c.isAutocommit()) {
					c.release();
				} else if (needRollback) {
//					c.setResponseHandler(new RollbackReleaseHandler());
//					c.rollback();
					c.quit();
				} else {
					c.release();
				}

            }
        }
    }

    public void releaseConnections(final boolean needRollback) {
        boolean debug = LOGGER.isDebugEnabled();
        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, debug, needRollback);
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
                .entrySet().iterator();
        while (itor.hasNext()) {
            BackendConnection theCon = itor.next().getValue();
            if (theCon == con) {
                itor.remove();
                con.release();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("realse connection " + con);
                }
                break;
            }
        }

    }

    /**
     * @return previous bound connection
     */
    public BackendConnection bindConnection(RouteResultsetNode key,
                                            BackendConnection conn) {
        // System.out.println("bind connection "+conn+
        // " to key "+key.getName()+" on sesion "+this);
        return target.put(key, conn);
    }
    
    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        if (conn == null) {
            return false;
        }

        boolean canReUse = false;
        // conn 是 slave db 的，并且 路由结果显示，本次sql可以重用该 conn
        if (conn.isFromSlaveDB() && (node.canRunnINReadDB(getSource().isAutocommit())
                && (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
            canReUse = true;
        }

        // conn 是 master db 的，并且路由结果显示，本次sql可以重用该conn
        if (!conn.isFromSlaveDB() && (node.getRunOnSlave() == null || !node.getRunOnSlave())) {
            canReUse = true;
        }

        if (canReUse) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found connections in session to use " + conn
                        + " for " + node);
            }
            conn.setAttachment(node);
            return true;
        } else {
            // slavedb connection and can't use anymore ,release it
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("release slave connection,can't be used in trasaction  "
                        + conn + " for " + node);
            }
            releaseConnection(node, LOGGER.isDebugEnabled(), false);
        }
        return false;
    }

//	public boolean tryExistsCon(final BackendConnection conn,
//			RouteResultsetNode node) {
//
//		if (conn == null) {
//			return false;
//		}
//		if (!conn.isFromSlaveDB()
//				|| node.canRunnINReadDB(getSource().isAutocommit())) {
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("found connections in session to use " + conn
//						+ " for " + node);
//			}
//			conn.setAttachment(node);
//			return true;
//		} else {
//			// slavedb connection and can't use anymore ,release it
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("release slave connection,can't be used in trasaction  "
//						+ conn + " for " + node);
//			}
//			releaseConnection(node, LOGGER.isDebugEnabled(), false);
//		}
//		return false;
//	}

    protected void kill() {
        boolean hooked = false;
        AtomicInteger count = null;
        Map<RouteResultsetNode, BackendConnection> killees = null;
        for (RouteResultsetNode node : target.keySet()) {
            BackendConnection c = target.get(node);
            if (c != null) {
                if (!hooked) {
                    hooked = true;
                    killees = new HashMap<RouteResultsetNode, BackendConnection>();
                    count = new AtomicInteger(0);
                }
                killees.put(node, c);
                count.incrementAndGet();
            }
        }
        if (hooked) {
            for (Entry<RouteResultsetNode, BackendConnection> en : killees
                    .entrySet()) {
                KillConnectionHandler kill = new KillConnectionHandler(
                        en.getValue(), this);
                MycatConfig conf = MycatServer.getInstance().getConfig();
                PhysicalDBNode dn = conf.getDataNodes().get(
                        en.getKey().getName());
                try {
                    dn.getConnectionFromSameSource(null, true, en.getValue(),
                            kill, en.getKey());
                } catch (Exception e) {
                    LOGGER.error(
                            "get killer connection failed for " + en.getKey(),
                            e);
                    kill.connectionError(e, null);
                }
            }
        }
    }

    private void clearHandlesResources() {
        SingleNodeHandler singleHander = singleNodeHandler;
        if (singleHander != null) {
            singleHander.clearResources();
            singleNodeHandler = null;
        }

	MultiNodeDdlHandler multiDdlHandler = multiNodeDdlHandler;
        if (multiDdlHandler != null) {
            	multiDdlHandler.clearResources();
		multiNodeDdlHandler = null;
        }
	
        MultiNodeQueryHandler multiHandler = multiNodeHandler;
        if (multiHandler != null) {
            multiHandler.clearResources();
            multiNodeHandler = null;
        }
		if (rollbackHandler != null) {
			rollbackHandler.clearResources();
		}
		if (commitHandler != null) {
			commitHandler.clearResources();
		}
    }

    public void clearResources(final boolean needRollback) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear session resources " + this);
        }
        this.releaseConnections(needRollback);
        needWaitFinished = false;
        clearHandlesResources();
        source.setTxstart(false);
        source.getAndIncrementXid();
    }

    public boolean closed() {
        return source.isClosed();
    }


	public void setXATXEnabled(boolean xaTXEnabled) {
		if (xaTXEnabled && this.xaTXID == null) {
			LOGGER.info("XA Transaction enabled ,con " + this.getSource());
			xaTXID = MycatServer.getInstance().genXATXID();
			xaState = TxState.TX_INITIALIZE_STATE;
		} else if (!xaTXEnabled && this.xaTXID != null) {
			LOGGER.info("XA Transaction disabled ,con " + this.getSource());
			xaTXID = null;
			xaState =null;
		}
	}

    public String getSessionXaID() {
        return xaTXID;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

	public MySQLConnection freshConn(MySQLConnection errConn, ResponseHandler queryHandler) {
		for (final RouteResultsetNode node : this.getTargetKeys()) {
			final MySQLConnection mysqlCon = (MySQLConnection) this.getTarget(node);
			if (errConn.equals(mysqlCon)) {
				MycatConfig conf = MycatServer.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				try {
					MySQLConnection newConn = (MySQLConnection) dn.getConnection(dn.getDatabase(),errConn.isAutocommit());
					newConn.setXaStatus(errConn.getXaStatus());
					if(!newConn.setResponseHandler(queryHandler)){
						return errConn;
					}
					this.bindConnection(node, newConn);
					return newConn;
				} catch (Exception e) {
					return errConn;
				}
			}
		}
		return errConn;
	}

	public MySQLConnection releaseExcept(TxState state) {
		MySQLConnection errConn = null;
		for (final RouteResultsetNode node : this.getTargetKeys()) {
			final MySQLConnection mysqlCon = (MySQLConnection) this.getTarget(node);
			if (mysqlCon.getXaStatus() != state) {
				this.releaseConnection(node, true, false);
			} else {
				errConn = mysqlCon;
			}
		}
		return errConn;
	}
	public void handleSpecial(RouteResultset rrs, String schema, boolean isSuccess){
		if (rrs.getSqlType() == ServerParse.DDL) {
			String sql = rrs.getSrcStatement();
			if (source.isTxstart()) {
				source.setTxstart(false);
				source.getAndIncrementXid();
			}
			MycatServer.getInstance().getTmManager().updateMetaData(schema, sql, isSuccess, true);
		}
	}
	public MemSizeController getJoinBufferMC() {
		return joinBufferMC;
	}

	public MemSizeController getOrderBufferMC() {
		return orderBufferMC;
	}

	public MemSizeController getOtherBufferMC() {
		return otherBufferMC;
	}
}
