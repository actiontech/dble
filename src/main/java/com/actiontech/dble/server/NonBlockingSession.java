/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.*;
import com.actiontech.dble.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.CommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.RollbackNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalRollbackNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XARollbackNodesHandler;
import com.actiontech.dble.backend.mysql.store.memalloc.MemSizeController;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.CostTimeProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.statistic.stat.QueryTimeCost;
import com.actiontech.dble.statistic.stat.QueryTimeCostContainer;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mycat
 */
public class NonBlockingSession implements Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);
    public static final int CANCEL_STATUS_INIT = 0;
    public static final int CANCEL_STATUS_COMMITTING = 1;
    static final int CANCEL_STATUS_CANCELING = 2;


    private final ServerConnection source;
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;
    private RollbackNodesHandler rollbackHandler;
    private CommitNodesHandler commitHandler;
    private volatile String xaTxId;
    private volatile TxState xaState;
    private boolean prepared;
    private volatile boolean needWaitFinished = false;
    // cancel status  0 - CANCEL_STATUS_INIT 1 - CANCEL_STATUS_COMMITTING  2 - CANCEL_STATUS_CANCELING
    private int cancelStatus = 0;

    private OutputHandler outputHandler;

    // the memory controller for join,orderby,other in this session
    private MemSizeController joinBufferMC;
    private MemSizeController orderBufferMC;
    private MemSizeController otherBufferMC;
    private QueryTimeCost queryTimeCost;
    private CostTimeProvider provider;
    private volatile boolean timeCost = false;
    private AtomicBoolean firstBackConRes = new AtomicBoolean(false);

    public NonBlockingSession(ServerConnection source) {
        this.source = source;
        this.target = new ConcurrentHashMap<>(2, 1f);
        this.joinBufferMC = new MemSizeController(4 * 1024 * 1024);
        this.orderBufferMC = new MemSizeController(4 * 1024 * 1024);
        this.otherBufferMC = new MemSizeController(4 * 1024 * 1024);
    }

    public void setOutputHandler(OutputHandler outputHandler) {
        this.outputHandler = outputHandler;
    }

    @Override
    public ServerConnection getSource() {
        return source;
    }

    public void setRequestTime() {
        if (DbleServer.getInstance().getConfig().getSystem().getCostTimeStat() == 0) {
            return;
        }
        timeCost = false;
        if (ThreadLocalRandom.current().nextInt(100) >= DbleServer.getInstance().getConfig().getSystem().getCostSamplePercent()) {
            return;
        }
        timeCost = true;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear");
        }
        queryTimeCost = new QueryTimeCost();
        provider = new CostTimeProvider();
        provider.beginRequest(source.getId());
        long requestTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frontend connection setRequestTime:" + requestTime);
        }
        queryTimeCost.setRequestTime(requestTime);
    }
    public void startProcess() {
        if (!timeCost) {
            return;
        }
        provider.startProcess(source.getId());
    }

    public void endParse() {
        if (!timeCost) {
            return;
        }
        provider.endParse(source.getId());
    }


    public void endRoute() {
        if (!timeCost) {
            return;
        }
        provider.endRoute(source.getId());
    }
    public void setResponseTime() {
        if (!timeCost) {
            return;
        }
        long responseTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("setResponseTime:" + responseTime);
        }
        queryTimeCost.getResponseTime().set(responseTime);
        provider.beginResponse(source.getId());
        QueryTimeCostContainer.getInstance().add(queryTimeCost);
    }

    public void setBackendRequestTime(long backendID) {
        if (!timeCost) {
            return;
        }
        QueryTimeCost backendCost = new QueryTimeCost();
        long requestTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("backend connection[" + backendID + "] setRequestTime:" + requestTime);
        }
        backendCost.setRequestTime(requestTime);
        queryTimeCost.getBackEndTimeCosts().put(backendID, backendCost);


    }

    public void setBackendResponseTime(long backendID) {
        if (!timeCost) {
            return;
        }
        QueryTimeCost backCost = queryTimeCost.getBackEndTimeCosts().get(backendID);
        long responseTime = System.nanoTime();
        if (backCost != null && backCost.getResponseTime().compareAndSet(0, responseTime)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("backend connection[" + backendID + "] setResponseTime:" + responseTime);
            }
            provider.resFromBack(source.getId());
            firstBackConRes.set(false);
        }
    }

    public void startExecuteBackend() {
        if (!timeCost) {
            return;
        }
        if (firstBackConRes.compareAndSet(false, true)) {
            provider.startExecuteBackend(source.getId());
        }
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
     * SET CANCELABLE STATUS
     */
    public synchronized boolean cancelableStatusSet(int value) {
        // in fact ,only CANCEL_STATUS_COMMITTING(1) or CANCEL_STATUS_CANCELING(2) need to judge
        if ((value | this.cancelStatus) > 2) {
            return false;
        }
        this.cancelStatus = value;
        return true;
    }

    @Override
    public void execute(RouteResultset rrs) {
        // clear prev execute resources
        clearHandlesResources();
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            LOGGER.debug(s.append(source).append(rrs).toString() + " rrs ");
        }
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
            if (rrs.isNeedOptimizer()) {
                try {
                    executeMultiSelect(rrs);
                } catch (MySQLOutPutException e) {
                    source.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
                }
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
            SingleNodeHandler singleNodeHandler = new SingleNodeHandler(rrs, this);
            if (this.isPrepared()) {
                singleNodeHandler.setPrepared(true);
            }
            try {
                singleNodeHandler.execute();
            } catch (Exception e) {
                handleSpecial(rrs, source.getSchema(), false);
                LOGGER.info(String.valueOf(source) + rrs, e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
            if (this.isPrepared()) {
                this.setPrepared(false);
            }
        } else {
            executeMultiResultSet(rrs);
        }
    }

    private void executeMultiResultSet(RouteResultset rrs) {
        if (rrs.getSqlType() == ServerParse.DDL) {
            /*
             * here, just a try! The sync is the superfluous, because there are heartbeats  at every backend node.
             * We don't do 2pc or 3pc. Because mysql(that is, resource manager) don't support that for ddl statements.
             */
            checkBackupStatus();
            MultiNodeDdlHandler multiNodeDdlHandler = new MultiNodeDdlHandler(rrs, this);
            try {
                multiNodeDdlHandler.execute();
            } catch (Exception e) {
                LOGGER.info(String.valueOf(source) + rrs, e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
        } else if (ServerParse.SELECT == rrs.getSqlType() && rrs.getGroupByCols() != null) {
            MultiNodeSelectHandler multiNodeSelectHandler = new MultiNodeSelectHandler(rrs, this);
            if (this.isPrepared()) {
                multiNodeSelectHandler.setPrepared(true);
            }
            try {
                multiNodeSelectHandler.execute();
            } catch (Exception e) {
                LOGGER.info(String.valueOf(source) + rrs, e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
            if (this.isPrepared()) {
                this.setPrepared(false);
            }
        } else {
            MultiNodeQueryHandler multiNodeHandler = new MultiNodeQueryHandler(rrs, this);
            if (this.isPrepared()) {
                multiNodeHandler.setPrepared(true);
            }
            try {
                multiNodeHandler.execute();
            } catch (Exception e) {
                LOGGER.info(String.valueOf(source) + rrs, e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
            if (this.isPrepared()) {
                this.setPrepared(false);
            }
        }
    }

    private void executeMultiResultSet(PlanNode node) {
        init();
        HandlerBuilder builder = new HandlerBuilder(node, this);
        try {
            builder.build(false); //no next
        } catch (SQLSyntaxErrorException e) {
            LOGGER.info(String.valueOf(source) + " execute plan is : " + node, e);
            source.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
        } catch (NoSuchElementException e) {
            LOGGER.info(String.valueOf(source) + " execute plan is : " + node, e);
            this.terminate();
            source.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
        } catch (MySQLOutPutException e) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(String.valueOf(source) + " execute plan is : " + node, e);
            }
            this.terminate();
            source.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            LOGGER.info(String.valueOf(source) + " execute plan is : " + node, e);
            this.terminate();
            source.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
        }
    }

    private void executeMultiSelect(RouteResultset rrs) {
        SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(this.getSource().getSchema(), this.getSource().getCharset().getResultsIndex(), DbleServer.getInstance().getTmManager(), false);
        visitor.visit(ast);
        PlanNode node = visitor.getTableNode();
        if (node.isCorrelatedSubQuery()) {
            throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Correlated Sub Queries is not supported ");
        }
        node.setSql(rrs.getStatement());
        node.setUpFields();
        PlanUtil.checkTablesPrivilege(source, node, ast);
        node = MyOptimizer.optimize(node);
        if (PlanUtil.containsSubQuery(node)) {
            final PlanNode finalNode = node;
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                //sub Query build will be blocked, so use ComplexQueryExecutor
                @Override
                public void run() {
                    executeMultiResultSet(finalNode);
                }
            });
        } else {
            if (!visitor.isContainSchema()) {
                node.setAst(ast);
            }
            executeMultiResultSet(node);
        }
    }


    private void init() {
        this.outputHandler = null;
    }

    public void onQueryError(byte[] message) {
        if (outputHandler != null) {
            outputHandler.backendConnError(message);
        } else {
            if (LOGGER.isDebugEnabled()) {
                source.close(new String(message));
            }
        }
    }

    private void resetCommitNodesHandler() {
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
        resetCommitNodesHandler();
        commitHandler.commit();
    }

    public void checkBackupStatus() {
        while (DbleServer.getInstance().isBackupLocked()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        needWaitFinished = true;
    }

    private void resetRollbackNodesHandler() {
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
        resetRollbackNodesHandler();
        rollbackHandler.rollback();
    }

    /**
     * lockTable
     *
     * @param rrs
     * @author songdabin
     * @date 2016-7-9
     */
    public void lockTable(RouteResultset rrs) {
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null ||
                nodes[0].getName().equals("")) {
            source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                    "No dataNode found ,please check tables defined in schema:" + source.getSchema());
            return;
        }
        LockTablesHandler handler = new LockTablesHandler(this, rrs);
        source.setLocked(true);
        try {
            handler.execute();
        } catch (Exception e) {
            LOGGER.info(String.valueOf(source) + rrs, e);
            source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
        }
    }

    /**
     * unLockTable
     *
     * @param sql
     * @author songdabin
     * @date 2016-7-9
     */
    public void unLockTable(String sql) {
        UnLockTablesHandler handler = new UnLockTablesHandler(this, this.source.isAutocommit(), sql);
        handler.execute();
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
    void initiativeTerminate() {

        for (BackendConnection node : target.values()) {
            node.terminate("client closed ");
        }
        target.clear();
        clearHandlesResources();
    }

    public void closeAndClearResources(String reason) {
        // XA MUST BE FINISHED
        if (source.isTxStart() && this.getXaState() != null && this.getXaState() != TxState.TX_INITIALIZE_STATE) {
            return;
        }
        for (BackendConnection node : target.values()) {
            node.terminate(reason);
        }
        target.clear();
        clearHandlesResources();
    }

    public void releaseConnectionIfSafe(BackendConnection conn, boolean needRollBack) {
        RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        if (node != null) {
            if ((this.source.isAutocommit() || conn.isFromSlaveDB()) && !this.source.isTxStart() && !this.source.isLocked()) {
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
                    //c.rollback();
                    c.quit();
                } else {
                    c.release();
                }

            }
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> iterator = target.entrySet().iterator();
        while (iterator.hasNext()) {
            BackendConnection theCon = iterator.next().getValue();
            if (theCon == con) {
                iterator.remove();
                con.release();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("realse connection " + con);
                }
                break;
            }
        }

    }

    public void releaseConnections(final boolean needRollback) {
        boolean debug = LOGGER.isDebugEnabled();
        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, debug, needRollback);
        }
    }

    /**
     * @return previous bound connection
     */
    public BackendConnection bindConnection(RouteResultsetNode key, BackendConnection conn) {
        return target.put(key, conn);
    }

    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        if (conn == null) {
            return false;
        }

        boolean canReUse = false;
        if (conn.isFromSlaveDB() && (node.canRunINReadDB(getSource().isAutocommit()) &&
                (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
            canReUse = true;
        }

        if (!conn.isFromSlaveDB()) {
            canReUse = true;
        }

        if (canReUse) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found connections in session to use " + conn + " for " + node);
            }
            conn.setAttachment(node);
            return true;
        } else {
            // slavedb connection and can't use anymore ,release it
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("release slave connection,can't be used in trasaction  " + conn + " for " + node);
            }
            releaseConnection(node, LOGGER.isDebugEnabled(), false);
        }
        return false;
    }

    protected void kill() {
        AtomicInteger count = new AtomicInteger(0);
        Map<RouteResultsetNode, BackendConnection> toKilled = new HashMap<>();

        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet()) {
            BackendConnection c = entry.getValue();
            if (c != null && !c.isDDL()) {
                toKilled.put(entry.getKey(), c);
                count.incrementAndGet();
            } else if (c != null && c.isDDL()) {
                //if the sql executing is a ddl,do not kill the query,just close the connection
                this.terminate();
                return;
            }
        }

        for (Entry<RouteResultsetNode, BackendConnection> en : toKilled.entrySet()) {
            KillConnectionHandler kill = new KillConnectionHandler(
                    en.getValue(), this);
            ServerConfig conf = DbleServer.getInstance().getConfig();
            PhysicalDBNode dn = conf.getDataNodes().get(
                    en.getKey().getName());
            try {
                dn.getConnectionFromSameSource(en.getValue().getSchema(), true, en.getValue(),
                        kill, en.getKey());
            } catch (Exception e) {
                LOGGER.info(
                        "get killer connection failed for " + en.getKey(),
                        e);
                kill.connectionError(e, null);
            }
        }
    }

    private void clearHandlesResources() {
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
        source.setTxStart(false);
        source.getAndIncrementXid();
    }

    public boolean closed() {
        return source.isClosed();
    }


    public void setXaTxEnabled(boolean xaTXEnabled) {
        if (xaTXEnabled && this.xaTxId == null) {
            LOGGER.info("XA Transaction enabled ,con " + this.getSource());
            xaTxId = DbleServer.getInstance().genXaTxId();
            xaState = TxState.TX_INITIALIZE_STATE;
        } else if (!xaTXEnabled && this.xaTxId != null) {
            LOGGER.info("XA Transaction disabled ,con " + this.getSource());
            xaTxId = null;
            xaState = null;
        }
    }

    public String getSessionXaID() {
        return xaTxId;
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
                ServerConfig conf = DbleServer.getInstance().getConfig();
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                try {
                    MySQLConnection newConn = (MySQLConnection) dn.getConnection(dn.getDatabase(), errConn.isAutocommit(), false);
                    newConn.setXaStatus(errConn.getXaStatus());
                    if (!newConn.setResponseHandler(queryHandler)) {
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

    public void handleSpecial(RouteResultset rrs, String schema, boolean isSuccess) {
        if (rrs.getSqlType() == ServerParse.DDL) {
            String sql = rrs.getSrcStatement();
            if (source.isTxStart()) {
                source.setTxStart(false);
                source.getAndIncrementXid();
            }
            DbleServer.getInstance().getTmManager().updateMetaData(schema, sql, isSuccess, true);
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
