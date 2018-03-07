/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalAutoCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.NormalAutoRollbackNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAutoCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAutoRollbackNodesHandler;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.alarm.AlarmCode;
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
    protected final NonBlockingSession session;
    private final boolean sessionAutocommit;
    protected long affectedRows;
    protected long selectRows;
    protected long startTime;
    protected long netInBytes;
    protected List<BackendConnection> errConnection;
    protected long netOutBytes;
    protected boolean prepared;
    protected ErrorPacket err;
    protected int fieldCount = 0;
    protected volatile boolean fieldsReturned;
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
        this.session = session;
    }

    private void recycleResources() {
        ByteBuffer buf = byteBuffer;
        if (buf != null) {
            session.getSource().recycle(byteBuffer);
            byteBuffer = null;
        }
    }

    protected void reset(int initCount) {
        super.reset(initCount);
        this.netInBytes = 0;
        this.netOutBytes = 0;
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
        startTime = System.currentTimeMillis();
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
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        conn.setSession(session);
        conn.execute(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart() && !node.isModifySQL());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.info("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;
        executeError(conn);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getSource().getCharset().getResults()));
        err = errPacket;
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
        errPacket.setPacketId(1); //TODO :CONFIRM ?++packetId??
        err = errPacket;
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            if (!conn.syncAndExecute()) {
                return;
            }
            if (--nodeCount <= 0) {
                handleDdl();
                session.handleSpecial(rrs, session.getSource().getSchema(), false);
                recycleResources();
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
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
                    session.handleSpecial(rrs, source.getSchema(), false);
                    handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
                    return;
                }
                session.handleSpecial(rrs, source.getSchema(), true);
                if (rrs.isLoadData()) {
                    byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
                    ok.setPacketId(++lastPackId); // OK_PACKET
                    ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0").getBytes());
                    source.getLoadDataInfileHandler().clear();
                } else {
                    ok.setPacketId(++packetId); // OK_PACKET
                }

                ok.setAffectedRows(affectedRows);
                ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                if (insertId > 0) {
                    ok.setInsertId(insertId);
                    source.setLastInsertId(insertId);
                }
                handleEndPacket(ok.toBytes(), AutoTxOperation.COMMIT, conn);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        this.netOutBytes += eof.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
        }
        if (fieldsReturned) {
            return;
        }
        lock.lock();
        try {
            if (fieldsReturned) {
                return;
            }
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
            if (!rrs.isCallStatement() || (rrs.isCallStatement() && rrs.getProcedure().isResultSimpleValue())) {
                if (this.sessionAutocommit && !session.getSource().isTxStart() && !session.getSource().isLocked()) { // clear all connections
                    session.releaseConnections(false);
                }

                if (this.isFail() || session.closed()) {
                    tryErrorFinished(true);
                    return;
                }
            }
            session.setResponseTime();
            writeEofResult(eof, source);
            doSqlStat(source);
        }

    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, BackendConnection conn) {
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
            RowDataPacket rowDataPkg = null;
            // cache primaryKey-> dataNode
            if (primaryKeyIndex != -1) {
                rowDataPkg = new RowDataPacket(fieldCount);
                rowDataPkg.read(row);
                String primaryKey = new String(rowDataPkg.fieldValues.get(primaryKeyIndex));
                LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
                if (pool != null) {
                    pool.putIfAbsent(primaryKeyTable, primaryKey, dataNode);
                }
            }
            row[3] = ++packetId;
            if (prepared) {
                if (rowDataPkg == null) {
                    rowDataPkg = new RowDataPacket(fieldCount);
                    rowDataPkg.read(row);
                }
                BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                binRowDataPk.read(fieldPackets, rowDataPkg);
                binRowDataPk.write(byteBuffer, session.getSource(), true);
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

    private void handleDdl() {
        if (rrs.getSqlType() != ServerParse.DDL || errConnection == null) {
            return;
        }

        StringBuilder s = new StringBuilder();
        s.append(rrs.toString());

        s.append(", failed={");
        for (int i = 0; i < errConnection.size(); i++) {
            BackendConnection conn = errConnection.get(i);
            s.append("\n ").append(FormatUtil.format(i + 1, 3));
            s.append(" -> ").append(conn.compactInfo());
        }
        s.append("\n}");

        LOGGER.warn(AlarmCode.CORE_DDL_WARN + s.toString());
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
            if (--nodeCount <= 0) {
                handleDdl();
                session.handleSpecial(rrs, session.getSource().getSchema(), false);
                recycleResources();
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeEofResult(byte[] eof, ServerConnection source) {
        lock.lock();
        try {
            eof[3] = ++packetId;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("last packet id:" + packetId);
            }
            byteBuffer = source.writeToBuffer(eof, byteBuffer);
            source.write(byteBuffer);
        } finally {
            lock.unlock();
        }
    }

    protected void doSqlStat(ServerConnection source) {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
            int resultSize = source.getWriteQueue().size() * DbleServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
            if (rrs != null && rrs.getStatement() != null) {
                netInBytes += rrs.getStatement().getBytes().length;
            }
            assert rrs != null;
            QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(),
                    rrs.getStatement(), selectRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(), resultSize);
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


    public void handleDataProcessException(Exception e) {
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
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    protected void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn) {
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
                session.releaseConnection(conn);
            }
            // Explicit Distributed Transaction
            if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                source.setTxInterrupt("ROLLBACK");
            }
            if (nodeCount == 0) {
                session.setResponseTime();
                session.getSource().write(data);
            }
        }
    }
}
