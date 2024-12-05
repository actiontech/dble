/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.LoadDataUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ExecutableHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.log.transaction.TxnLogHelper;
import com.oceanbase.obsharding_d.meta.DDLProxyMetaManager;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.stat.QueryResultDispatcher;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.oceanbase.obsharding_d.singleton.DDLTraceHelper.Stage.exec_ddl_sql;

/**
 * this handler that can receive and process multiple nodes
 */
public abstract class BaseDDLHandler implements ResponseHandler, ExecutableHandler, LoadDataResponseHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger("DDL_TRACE");

    protected static final int STATUS_INIT = 0;
    protected static final int STATUS_CONN_ERR = 1;
    protected static final int STATUS_CONN_CLOSE = 2;
    protected static final int STATUS_ERR = 3;
    // protected int STATUS_SESSION_CLOSE = 4;
    protected static final int STATUS_OK = 9;

    protected final NonBlockingSession session;
    protected RouteResultset preRrs; // in ddl prepare, 'select 1'
    protected RouteResultset rrs;
    protected RouteResultset oriRrs;
    protected final boolean sessionAutocommit;

    private long netOutBytes = 0;
    private long resultSize = 0;

    protected final ReentrantLock lock = new ReentrantLock();
    protected HashMap<RouteResultsetNode, Integer> nodeResponseStatus = Maps.newHashMap();
    protected Set<MySQLResponseService> closedConnSet = new HashSet<>(1);
    protected AtomicBoolean writeToClientFlag = new AtomicBoolean(false);
    protected AtomicBoolean specialHandleFlag = new AtomicBoolean(false); // execute special handling only once
    protected volatile String errMsg;
    protected volatile ErrorPacket err;

    protected String traceMessage = "execute-for-ddl";
    protected DDLTraceHelper.Stage stage = exec_ddl_sql;

    protected Object attachment;
    protected ImplicitlyCommitCallback implicitlyCommitCallback;

    public BaseDDLHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment, ImplicitlyCommitCallback implicitlyCommitCallback) {
        if (session == null)
            throw new IllegalArgumentException("session is null!");
        if (rrs.getNodes() == null)
            throw new IllegalArgumentException("routeNode is null!");

        this.session = session;
        this.rrs = rrs;
        this.oriRrs = rrs;
        this.sessionAutocommit = session.getShardingService().isAutocommit();
        this.attachment = attachment;
        this.implicitlyCommitCallback = implicitlyCommitCallback;
    }

    @Override
    public void execute() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), traceMessage);
        try {
            Arrays.stream(rrs.getNodes()).forEach(b -> nodeResponseStatus.put(b, STATUS_INIT));
            String nodesStr = Strings.join(
                    nodeResponseStatus.keySet().
                            stream().
                            map(n -> n.getName()).
                            collect(Collectors.toSet()),
                    ',');
            String log0;
            if (stage == exec_ddl_sql)
                log0 = "This ddl will be executed separately in the shardingNodes[" + nodesStr + "]";
            else
                log0 = "Start execute 'select 1' to detect a valid connection for shardingNodes[" + nodesStr + "]";

            DDLTraceHelper.log(session.getShardingService(), d -> d.info(stage, DDLTraceHelper.Status.start, log0));

            for (final RouteResultsetNode node : rrs.getNodes()) {
                BackendConnection conn = session.getTarget(node);
                DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.start, "In shardingNode[" + node.getName() + "],about to execute sql{" + node.getStatement() + "}"));
                if (session.tryExistsCon(conn, node)) {
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    executeInExistsConnection(conn, node);
                } else {
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                    dn.getConnection(dn.getDatabase(), isMustWrite(), sessionAutocommit, node, this, node);
                }
            }
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    protected boolean isMustWrite() {
        return session.getShardingService().isInTransaction();
    }

    protected void executeInExistsConnection(BackendConnection conn, RouteResultsetNode node) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-in-exists-connection");
        try {
            TraceManager.crossThread(conn.getBackendService(), "backend-response-service", session.getShardingService());
            innerExecute(conn, node);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    protected void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed()) return;
        DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.get_conn, "Get " + conn.toString()));
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart());
    }

    @Override
    public void clearAfterFailExecute() {
        clearResources();
    }

    @Override
    public void writeRemainBuffer() {
    }

    @Override
    public void connectionError(Throwable e, Object attachment0) {
        lock.lock();
        try {
            RouteResultsetNode node = (RouteResultsetNode) attachment0;
            String errMsg0 = "can't connect to shardingNode[" + node.getName() + "], due to " + e.getMessage();
            DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.fail, errMsg0));
            LOGGER.warn(errMsg0);

            setErrPkg(errMsg0, ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
            if (decrementToZero(node, STATUS_CONN_ERR) && writeToClientFlag.compareAndSet(false, true)) {
                specialHandling(false, this.getErrMsg());
                this.err.setPacketId(session.getShardingService().nextPacketId());
                handleEndPacket(this.err);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-error-response");
        TraceManager.finishSpan(service, traceObject);

        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("unfinished sync");
        }
        lock.lock();
        try {
            MySQLResponseService responseService = (MySQLResponseService) service;
            final RouteResultsetNode node = (RouteResultsetNode) responseService.getAttachment();

            ErrorPacket tmpErrPacket = new ErrorPacket();
            tmpErrPacket.read(data);
            String errMsg0 = new String(tmpErrPacket.getMessage());
            DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.fail, errMsg0));

            setErrPkg(errMsg0, tmpErrPacket.getErrNo());
            if (decrementToZero(node, STATUS_ERR) && writeToClientFlag.compareAndSet(false, true)) {
                specialHandling(false, this.getErrMsg());
                this.err.setPacketId(session.getShardingService().nextPacketId());
                handleEndPacket(this.err);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-response");
        TraceManager.finishSpan(service, traceObject);

        this.netOutBytes += data.length;
        MySQLResponseService responseService = (MySQLResponseService) service;
        boolean executeResponse = responseService.syncAndExecute();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }

        if (executeResponse) {
            this.resultSize += data.length;
            session.setBackendResponseEndTime(responseService);
            final RouteResultsetNode node = (RouteResultsetNode) responseService.getAttachment();
            final ShardingService shardingService = session.getShardingService();
            lock.lock();
            try {
                DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.succ));
                if (!decrementToZero(node, STATUS_OK)) return;
                if (writeToClientFlag.compareAndSet(false, true)) {
                    if (isErr()) {
                        specialHandling(false, this.getErrMsg());
                        this.err.setPacketId(shardingService.nextPacketId());
                        handleEndPacket(this.err);
                        session.resetMultiStatementStatus();
                    } else {
                        boolean metaInitial = specialHandling(true, null);
                        MySQLPacket packet;
                        if (metaInitial) {
                            OkPacket ok = new OkPacket();
                            ok.read(data);
                            ok.setPacketId(shardingService.nextPacketId()); // OK_PACKET
                            ok.setMessage(null);
                            ok.setAffectedRows(0);
                            ok.setServerStatus(shardingService.isAutocommit() ? 2 : 1);
                            shardingService.setLastInsertId(ok.getInsertId());
                            session.setRowCount(0);
                            packet = ok;
                        } else {
                            ErrorPacket err0 = new ErrorPacket();
                            err0.setPacketId(shardingService.nextPacketId());
                            err0.setErrNo(ErrorCode.ER_META_DATA);
                            String msg = "DDL executed successfully, maybe there's something wrong with update_ddl_metadata or notice_cluster_ddl. You are advised to analyze OBsharding-D log first, then decide whether to execute 'reload @@metadata'.";
                            err0.setMessage(StringUtil.encode(msg, shardingService.getCharset().getResults()));
                            packet = err0;
                        }
                        QueryResultDispatcher.doSqlStat(rrs, session, 0, netOutBytes, resultSize);
                        handleEndPacket(packet);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String closeReason0) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-connection-closed");
        TraceManager.finishSpan(service, traceObject);

        MySQLResponseService responseService = (MySQLResponseService) service;
        final RouteResultsetNode node = (RouteResultsetNode) responseService.getAttachment();
        if (checkIsAlreadyClosed(node, responseService)) return;

        LOGGER.warn("backend connect {}, conn info:{}", closeReason0, service);
        DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.fail, closeReason0));
        String errMsg0 = new StringBuilder("Connection {dbInstance[").
                append(service.getConnection().getHost()).
                append(":").append(service.getConnection().getPort()).append("],Schema[").append(responseService.getSchema()).
                append("],threadID[").append(responseService.getConnection().getThreadId()).append("]} was closed ,reason is [").
                append(closeReason0).append("]").toString();

        lock.lock();
        try {
            responseService.setResponseHandler(null);
            setErrPkg(errMsg0, ErrorCode.ER_ABORTING_CONNECTION);
            if (decrementToZero(node, STATUS_CONN_CLOSE) && writeToClientFlag.compareAndSet(false, true)) {
                specialHandling(false, this.getErrMsg());
                this.err.setPacketId(session.getShardingService().nextPacketId());
                handleEndPacket(this.err);
            }
        } finally {
            lock.unlock();
        }
    }

    public void executeFail(String errInfo) {
        lock.lock();
        try {
            setErrPkg(errInfo, ErrorCode.ERR_HANDLE_DATA);
            if (writeToClientFlag.compareAndSet(false, true)) {
                specialHandling(false, this.getErrMsg());
                clearAfterFailExecute();
                this.err.setPacketId(session.getShardingService().nextPacketId());
                handleEndPacket(this.err);
            }
        } finally {
            lock.unlock();
        }
    }

    protected boolean checkIsAlreadyClosed(final RouteResultsetNode node, final MySQLResponseService mysqlResponseService) {
        lock.lock();
        try {
            if (closedConnSet.contains(mysqlResponseService)) {
                nodeResponseStatus.put(node, STATUS_CONN_CLOSE);
                return true;
            } else {
                closedConnSet.add(mysqlResponseService);
            }
            session.getTargetMap().remove(node);
            return false;
        } finally {
            lock.unlock();
        }
    }

    protected boolean decrementToZero(final RouteResultsetNode currNode, int currStatus0) {
        lock.lock();
        try {
            if (nodeResponseStatus.containsKey(currNode))
                nodeResponseStatus.put(currNode, currStatus0);
            return nodeResponseStatus.values().stream().allMatch(status -> status > STATUS_INIT);
        } finally {
            lock.unlock();
        }
    }

    protected boolean isErr() {
        return err != null;
    }

    protected String getErrMsg() {
        return errMsg;
    }

    protected void setErrPkg(String errMsg0, int errorCode0) {
        if (this.err == null) {
            this.err = new ErrorPacket();
        }
        err.setErrNo(errorCode0 == 0 ? ErrorCode.ER_UNKNOWN_ERROR : errorCode0);
        errMsg = errMsg0;
        err.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
    }

    protected void clearResources() {
        nodeResponseStatus.clear();
        closedConnSet.clear();
    }

    protected void handleEndPacket(MySQLPacket packet) {
        session.clearResources(false);
        this.clearResources();
        DDLTraceHelper.finish(session.getShardingService());
        packet.write(session.getSource());
    }

    protected boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution, clear resources " + session);
            }
            String msg = "Current session closed";
            specialHandling(false, msg);
            DDLTraceHelper.finish(session.getShardingService());
            session.clearResources(false);
            this.clearResources();
            return true;
        } else {
            return false;
        }
    }

    public final boolean specialHandling(boolean isExecSucc, String errInfo) {
        if (specialHandleFlag.compareAndSet(false, true)) {
            if (implicitlyCommitCallback != null)
                implicitlyCommitCallback.callback();

            if (preRrs != null)
                TxnLogHelper.putTxnLog(session.getShardingService(), this.preRrs);
            TxnLogHelper.putTxnLog(session.getShardingService(), this.rrs);

            if (isExecSucc)
                DDLTraceHelper.log(session.getShardingService(), d -> d.info(stage, DDLTraceHelper.Status.succ));
            else
                DDLTraceHelper.log(session.getShardingService(), d -> d.info(stage, DDLTraceHelper.Status.fail, errInfo));

            if (oriRrs.getSchema() == null) {
                LOGGER.info("Hint ddl do not update the meta and cluster notify");
                return true;
            }
            if (oriRrs.isOnline()) {
                LOGGER.info("Online ddl skip updating meta and cluster notify");
                return true;
            }
            return specialHandling0(isExecSucc);
        }
        // else ignore
        return true;
    }

    // special handling: updating meta and cluster notify
    protected boolean specialHandling0(boolean isExecSucc) {
        return DDLProxyMetaManager.Originator.updateMetaData(session.getShardingService(), oriRrs, isExecSucc);
    }

    @Override
    public void requestDataResponse(byte[] data, @NotNull MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setPreRrs(RouteResultset preRrs) {
        this.preRrs = preRrs;
    }
}
