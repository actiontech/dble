/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalAutoCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalAutoRollbackNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAutoCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAutoRollbackNodesHandler;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);
    protected final RouteResultset rrs;
    private final boolean sessionAutocommit;
    private long affectedRows;
    long selectRows;
    private List<BackendConnection> errConnection;
    private long netOutBytes;
    private long resultSize;
    protected boolean prepared;
    protected ErrorPacket err;
    protected int fieldCount = 0;
    volatile boolean fieldsReturned;
    private long insertId;
    private String primaryKeyTable = null;
    private int primaryKeyIndex = -1;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    private volatile ByteBuffer byteBuffer;
    private Set<BackendConnection> closedConnSet;

    public MultiNodeQueryHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multi node query " + rrs.getStatement());
        }
        this.rrs = rrs;
        if (ServerParse.SELECT == rrs.getSqlType()) {
            byteBuffer = session.getSource().allocate();
        }
        this.sessionAutocommit = session.getSource().isAutocommit();
    }

    protected void reset(int initCount) {
        super.reset(initCount);
        if (rrs.isLoadData()) {
            packetId = session.getSource().getLoadDataInfileHandler().getLastPackId();
        }
        this.netOutBytes = 0;
        this.resultSize = 0;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.reset(rrs.getNodes().length);
            this.fieldsReturned = false;
            this.affectedRows = 0L;
            this.insertId = 0L;
        } finally {
            lock.unlock();
        }
        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            if (node.isModifySQL()) {
                sb.append("[").append(node.getName()).append("]").append(node.getStatement()).append(";\n");
            }
        }
        if (sb.length() > 0) {
            TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
        }
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                node.setRunOnSlave(rrs.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                // create new connection
                node.setRunOnSlave(rrs.getRunOnSlave());
                PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session, this.rrs.getSqlType() != ServerParse.DDL) && this.rrs.getSqlType() != ServerParse.DDL) {
            return;
        }
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        mysqlCon.setResponseHandler(this);
        mysqlCon.setSession(session);
        mysqlCon.executeMultiNode(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart() && !node.isModifySQL());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.warn("backend connect" + reason + ", conn info:" + conn);
        ErrorPacket errPacket = new ErrorPacket();
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        reason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        executeError(conn);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("Backend connect Error, Connection info:" + conn, e);
        ErrorPacket errPacket = new ErrorPacket();
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId);
        errPacket.setErrNo(ErrorCode.ER_DATA_HOST_ABORTING_CONNECTION);
        String errMsg = "Backend connect Error, Connection{DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        executeError(conn);
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId); //just for normal error
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (--nodeCount == 0) {
                session.handleSpecial(rrs, false, getDDLErrorInfo());
                packetId++;
                if (byteBuffer != null) {
                    session.getSource().write(byteBuffer);
                }
                handleEndPacket(errPacket.toBytes(), AutoTxOperation.ROLLBACK, conn, false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        this.netOutBytes += data.length;
        boolean executeResponse = conn.syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + conn);
        }
        if (executeResponse) {
            this.resultSize += data.length;
            session.setBackendResponseEndTime((MySQLConnection) conn);
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            lock.lock();
            try {
                // the affected rows of global table will use the last node's response
                if (!rrs.isGlobalTable()) {
                    affectedRows += ok.getAffectedRows();
                } else {
                    affectedRows = ok.getAffectedRows();
                }
                if (ok.getInsertId() > 0) {
                    insertId = (insertId == 0) ? ok.getInsertId() : Math.min(
                            insertId, ok.getInsertId());
                }
                if (--nodeCount > 0)
                    return;
                if (isFail()) {
                    session.handleSpecial(rrs, false);
                    session.resetMultiStatementStatus();
                    handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn, false);
                    return;
                }
                boolean metaInited = session.handleSpecial(rrs, true);
                if (!metaInited) {
                    executeMetaDataFailed(conn);
                    return;
                }
                ok.setPacketId(++packetId); // OK_PACKET
                if (rrs.isLoadData()) {
                    ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0").getBytes());
                    source.getLoadDataInfileHandler().clear();
                } else {
                    ok.setMessage(null);
                }

                ok.setAffectedRows(affectedRows);
                ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                if (insertId > 0) {
                    ok.setInsertId(insertId);
                    source.setLastInsertId(insertId);
                }
                session.multiStatementPacket(ok, packetId);
                doSqlStat();
                handleEndPacket(ok.toBytes(), AutoTxOperation.COMMIT, conn, true);
            } finally {
                lock.unlock();
            }
        }
    }

    private void executeMetaDataFailed(BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_META_DATA);
        String errMsg = "Create TABLE OK, but generate metedata failed for the current druid parser can not recognize part of the sql";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        session.multiStatementPacket(errPacket, packetId);
        doSqlStat();
        handleEndPacket(errPacket.toBytes(), AutoTxOperation.COMMIT, conn, false);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
        }
        this.netOutBytes += eof.length;
        if (fieldsReturned) {
            return;
        }
        lock.lock();
        try {
            if (fieldsReturned) {
                return;
            }
            this.resultSize += header.length;
            for (byte[] field : fields) {
                this.resultSize += field.length;
            }
            this.resultSize += eof.length;
            fieldsReturned = true;
            executeFieldEof(header, fields, eof);
        } catch (Exception e) {
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end response " + conn);
        }

        this.netOutBytes += eof.length;
        if (errorResponse.get()) {
            return;
        }
        final ServerConnection source = session.getSource();
        if (!rrs.isCallStatement()) {
            if (clearIfSessionClosed(session)) {
                return;
            } else {
                session.releaseConnectionIfSafe(conn, false);
            }
        }

        if (decrementCountBy(1)) {
            this.resultSize += eof.length;
            if (!rrs.isCallStatement()) {
                if (this.sessionAutocommit && !session.getSource().isTxStart() && !session.getSource().isLocked()) { // clear all connections
                    session.releaseConnections(false);
                }

                if (this.isFail()) {
                    session.setResponseTime(false);
                    session.resetMultiStatementStatus();
                    source.write(byteBuffer);
                    ErrorPacket errorPacket = createErrPkg(this.error);
                    handleEndPacket(errorPacket.toBytes(), AutoTxOperation.ROLLBACK, conn, false); //todo :optimized
                    return;
                }
            }
            session.multiStatementPacket(eof, packetId);
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            writeEofResult(eof, source);
            session.multiStatementNextSql(multiStatementFlag);
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, BackendConnection conn) {
        this.netOutBytes += row.length;
        if (errorResponse.get()) {
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

            if (rrs.getLimitSize() >= 0) {
                if (selectRows <= rrs.getLimitStart() ||
                        (selectRows > (rrs.getLimitStart() < 0 ? 0 : rrs.getLimitStart()) + rrs.getLimitSize())) {
                    return false;
                }
            }
            this.resultSize += row.length;
            row[3] = ++packetId;
            RowDataPacket rowDataPkg = null;
            // cache primaryKey-> dataNode
            if (primaryKeyIndex != -1) {
                rowDataPkg = new RowDataPacket(fieldCount);
                rowDataPkg.read(row);
                byte[] key = rowDataPkg.fieldValues.get(primaryKeyIndex);
                if (key != null) {
                    String primaryKey = new String(rowDataPkg.fieldValues.get(primaryKeyIndex));
                    LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
                    if (pool != null) {
                        pool.putIfAbsent(primaryKeyTable, primaryKey, dataNode);
                    }
                }
            }
            if (prepared) {
                if (rowDataPkg == null) {
                    rowDataPkg = new RowDataPacket(fieldCount);
                    rowDataPkg.read(row);
                }
                BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                binRowDataPk.read(fieldPackets, rowDataPkg);
                binRowDataPk.setPacketId(rowDataPkg.getPacketId());
                byteBuffer = binRowDataPk.write(byteBuffer, session.getSource(), true);
            } else {
                byteBuffer = session.getSource().writeToBuffer(row, byteBuffer);
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
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
    }

    @Override
    public void writeQueueAvailable() {
    }

    @Override
    public void requestDataResponse(byte[] data, BackendConnection conn) {
        LoadDataUtil.requestFileDataResponse(data, conn);
    }

    private String getDDLErrorInfo() {
        if (rrs.getSqlType() != ServerParse.DDL || errConnection == null) {
            return "";
        }

        StringBuilder s = new StringBuilder();
        s.append("{");
        for (int i = 0; i < errConnection.size(); i++) {
            BackendConnection conn = errConnection.get(i);
            s.append("\n ").append(FormatUtil.format(i + 1, 3));
            s.append(" -> ").append(conn.compactInfo());
        }
        s.append("\n}");

        return s.toString();
    }

    private void executeError(BackendConnection conn) {
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (--nodeCount == 0) {
                session.handleSpecial(rrs, false);
                packetId++;
                if (byteBuffer == null) {
                    handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn, false);
                } else {
                    session.getSource().write(byteBuffer);
                    handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn, false);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeEofResult(byte[] eof, ServerConnection source) {

        eof[3] = ++packetId;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("last packet id:" + packetId);
        }
        byteBuffer = source.writeToBuffer(eof, byteBuffer);
        session.setResponseTime(true);
        doSqlStat();
        source.write(byteBuffer);
    }

    void doSqlStat() {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs != null && rrs.getStatement() != null) {
                netInBytes += rrs.getStatement().getBytes().length;
            }
            assert rrs != null;
            QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(),
                    rrs.getStatement(), selectRows, netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        ServerConnection source = session.getSource();
        fieldCount = fields.size();
        header[3] = ++packetId;
        byteBuffer = source.writeToBuffer(header, byteBuffer);
        String primaryKey = null;
        if (rrs.hasPrimaryKeyToCache()) {
            String[] items = rrs.getPrimaryKeyItems();
            primaryKeyTable = items[0];
            primaryKey = items[1];
        }

        for (int i = 0, len = fieldCount; i < len; ++i) {
            byte[] field = fields.get(i);
            FieldPacket fieldPkg = new FieldPacket();
            fieldPkg.read(field);
            if (rrs.getSchema() != null) {
                fieldPkg.setDb(rrs.getSchema().getBytes());
            }
            if (rrs.getTableAlias() != null) {
                fieldPkg.setTable(rrs.getTableAlias().getBytes());
            }
            if (rrs.getTable() != null) {
                fieldPkg.setOrgTable(rrs.getTable().getBytes());
            }
            fieldPackets.add(fieldPkg);
            fieldCount = fields.size();
            if (primaryKey != null && primaryKeyIndex == -1) {
                // find primary key index
                String fieldName = new String(fieldPkg.getName());
                if (primaryKey.equalsIgnoreCase(fieldName)) {
                    primaryKeyIndex = i;
                }
            }
            fieldPkg.setPacketId(++packetId);
            byteBuffer = fieldPkg.write(byteBuffer, source, false);
        }
        eof[3] = ++packetId;
        byteBuffer = source.writeToBuffer(eof, byteBuffer);
    }


    void handleDataProcessException(Exception e) {
        if (!errorResponse.get()) {
            this.error = e.toString();
            LOGGER.info("caught exception ", e);
            setFail(e.toString());
            this.tryErrorFinished(true);
        }
    }

    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn);
            } else {
                if (closedConnSet.contains(conn)) {
                    return true;
                }
                closedConnSet.add(conn);
            }
            if (this.rrs.getSqlType() == ServerParse.DDL) {
                this.getSession().getTargetMap().remove(conn.getAttachment());
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }


    void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn, boolean isSuccess) {
        ServerConnection source = session.getSource();
        if (source.isAutocommit() && !source.isTxStart() && conn.isModifiedSQLExecuted()) {
            if (nodeCount < 0) {
                return;
            }
            //Implicit Distributed Transaction,send commit or rollback automatically
            if (txOperation == AutoTxOperation.COMMIT) {
                if (!conn.isDDL()) {
                    session.checkBackupStatus();
                }
                session.setBeginCommitTime();
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
            boolean inTransaction = !source.isAutocommit() || source.isTxStart();
            if (!inTransaction) {
                for (BackendConnection errConn : errConnection) {
                    session.releaseConnection(errConn);
                }
            }

            if (nodeCount == 0) {
                // Explicit Distributed Transaction
                if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                    source.setTxInterrupt("ROLLBACK");
                }
                session.setResponseTime(isSuccess);
                session.getSource().write(data);
                session.multiStatementNextSql(session.getIsMultiStatement().get());
            }
        }
    }
}
