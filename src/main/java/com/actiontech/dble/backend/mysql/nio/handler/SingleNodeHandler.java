/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.CacheService;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, LoadDataResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    private final RouteResultsetNode node;
    protected final RouteResultset rrs;
    protected final NonBlockingSession session;

    // only one thread access at one time no need lock
    protected volatile byte packetId;
    protected volatile ByteBuffer buffer;
    protected long netOutBytes;
    private long resultSize;
    long selectRows;

    private String cacheKeyTable = null;
    private int cacheKeyIndex = -1;

    private boolean prepared;
    private int fieldCount;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    private volatile boolean connClosed = false;
    protected AtomicBoolean writeToClient = new AtomicBoolean(false);

    public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];
        if (node == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
    }

    public void execute() throws Exception {
        connClosed = false;
        if (rrs.isLoadData()) {
            packetId = session.getSource().getLoadDataInfileHandler().getLastPackId();
        } else {
            packetId = (byte) session.getPacketId().get();
        }
        RouteResultsetNode finalNode = null;
        if (session.getTargetCount() > 0) {
            BackendConnection conn = session.getTarget(node);
            if (conn == null && rrs.isGlobalTable() && rrs.getGlobalBackupNodes() != null) {
                // read only trx for global table
                for (String dataNode : rrs.getGlobalBackupNodes()) {
                    RouteResultsetNode tmpNode = new RouteResultsetNode(dataNode, rrs.getSqlType(), rrs.getStatement());
                    conn = session.getTarget(tmpNode);
                    if (conn != null) {
                        finalNode = tmpNode;
                        break;
                    }
                }
            }
            node.setRunOnSlave(rrs.getRunOnSlave());
            if (session.tryExistsCon(conn, finalNode == null ? node : finalNode)) {
                execute(conn);
                return;
            }
        }

        // create new connection
        node.setRunOnSlave(rrs.getRunOnSlave());
        ServerConfig conf = DbleServer.getInstance().getConfig();
        PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
        dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), session.getSource().isAutocommit(), node, this, node);
    }

    protected void execute(BackendConnection conn) {
        if (clearIfSessionClosed()) {
            return;
        }
        conn.setResponseHandler(this);
        conn.setSession(session);
        boolean isAutocommit = session.getSource().isAutocommit() && !session.getSource().isTxStart();
        if (!isAutocommit && node.isModifySQL()) {
            TxnLogHelper.putTxnLog(session.getSource(), node.getStatement());
        }
        session.readyToDeliver();
        session.setPreExecuteEnd(false);
        conn.execute(node, session.getSource(), isAutocommit);
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        session.bindConnection(node, conn);
        execute(conn);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("Backend connect Error, Connection info:" + conn, e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DATA_HOST_ABORTING_CONNECTION);
        String errMsg = "Backend connect Error, Connection{DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        backConnectionErr(errPacket, conn, true);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        err.setPacketId(++packetId);
        backConnectionErr(err, conn, conn.syncAndExecute());
        session.resetMultiStatementStatus();
    }

    public void recycleBuffer() {
        lock.lock();
        try {
            if (buffer != null) {
                session.getSource().recycle(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        lock.lock();
        try {
            ServerConnection source = session.getSource();
            String errUser = source.getUser();
            String errHost = source.getHost();
            int errPort = source.getLocalPort();

            String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
            LOGGER.info("execute sql err :" + errMsg + " con:" + conn +
                    " frontend host:" + errHost + "/" + errPort + "/" + errUser);

            if (conn.isClosed()) {
                if (conn.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            } else if (syncFinished) {
                session.releaseConnectionIfSafe(conn, false);
            } else {
                conn.closeWithoutRsp("unfinished sync");
                if (conn.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            }
            source.setTxInterrupt(errMsg);

            if (writeToClient.compareAndSet(false, true)) {
                if (buffer != null) {
                    /* SELECT 9223372036854775807 + 1;    response: field_count, field, eof, err */
                    buffer = source.writeToBuffer(errPkg.toBytes(), buffer);
                    session.setResponseTime(false);
                    source.write(buffer);
                } else {
                    errPkg.write(source);
                }
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * insert/update/delete
     * <p>
     * okResponse():
     * read data, make an OKPacket, write to writeQueue in FrontendConnection by ok.write(source)
     */
    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        this.netOutBytes += data.length;

        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            this.resultSize += data.length;
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            if (rrs.isLoadData()) {
                byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
                ok.setPacketId(++lastPackId); // OK_PACKET
                source.getLoadDataInfileHandler().clear();

            } else {
                ok.setPacketId(++packetId); // OK_PACKET
            }
            ok.setMessage(null);
            ok.setServerStatus(source.isAutocommit() ? 2 : 1);
            source.setLastInsertId(ok.getInsertId());
            session.setBackendResponseEndTime((MySQLConnection) conn);
            session.releaseConnectionIfSafe(conn, false);
            session.setResponseTime(true);
            session.multiStatementPacket(ok, packetId);
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            doSqlStat();

            if (rrs.isCallStatement() || writeToClient.compareAndSet(false, true)) {
                ok.write(source);
            }
            session.multiStatementNextSql(multiStatementFlag);
        }
    }

    /**
     * select
     * <p>
     * write EOF to Queue
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;
        // if it's call statement,it will not release connection
        if (!rrs.isCallStatement()) {
            session.releaseConnectionIfSafe(conn, false);
        }

        eof[3] = ++packetId;
        session.multiStatementPacket(eof, packetId);
        ServerConnection source = session.getSource();
        session.setResponseTime(true);
        final boolean multiStatementFlag = session.getIsMultiStatement().get();
        doSqlStat();
        lock.lock();
        try {
            if (writeToClient.compareAndSet(false, true)) {
                buffer = source.writeToBuffer(eof, buffer);
                source.write(buffer);
            }
        } finally {
            lock.unlock();
        }
        session.multiStatementNextSql(multiStatementFlag);
    }

    public void doSqlStat() {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs.getStatement() != null) {
                netInBytes = rrs.getStatement().getBytes().length;
            }
            QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(), rrs.getStatement(), selectRows,
                    netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        this.resultSize += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
            this.resultSize += field.length;
        }
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;


        String cacheKey = null;
        if (rrs.hasCacheKeyToCache()) {
            String[] items = rrs.getCacheKeyItems();
            cacheKeyTable = items[0];
            cacheKey = items[1];
        }

        header[3] = ++packetId;

        ServerConnection source = session.getSource();
        lock.lock();
        try {
            if (!writeToClient.get()) {
                buffer = session.getSource().allocate();
                buffer = source.writeToBuffer(header, buffer);
                for (int i = 0, len = fields.size(); i < len; ++i) {
                    byte[] field = fields.get(i);
                    field[3] = ++packetId;

                    // save field
                    FieldPacket fieldPk = new FieldPacket();
                    fieldPk.read(field);
                    if (rrs.getSchema() != null) {
                        fieldPk.setDb(rrs.getSchema().getBytes());
                    }
                    if (rrs.getTableAlias() != null) {
                        fieldPk.setTable(rrs.getTableAlias().getBytes());
                    }
                    if (rrs.getTable() != null) {
                        fieldPk.setOrgTable(rrs.getTable().getBytes());
                    }
                    fieldPackets.add(fieldPk);

                    // find cache key index
                    if (cacheKey != null && cacheKeyIndex == -1) {
                        String fieldName = new String(fieldPk.getName());
                        if (cacheKey.equalsIgnoreCase(fieldName)) {
                            cacheKeyIndex = i;
                        }
                    }

                    buffer = fieldPk.write(buffer, source, false);
                }

                fieldCount = fieldPackets.size();

                eof[3] = ++packetId;
                buffer = source.writeToBuffer(eof, buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {

        this.netOutBytes += row.length;
        this.resultSize += row.length;
        this.selectRows++;
        row[3] = ++packetId;

        RowDataPacket rowDataPk = null;
        // cache cacheKey-> dataNode
        if (cacheKeyIndex != -1) {
            rowDataPk = new RowDataPacket(fieldCount);
            rowDataPk.read(row);
            byte[] key = rowDataPk.fieldValues.get(cacheKeyIndex);
            if (key != null) {
                String cacheKey = new String(key);
                RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
                LayerCachePool pool = CacheService.getTableId2DataNodeCache();
                if (pool != null) {
                    pool.putIfAbsent(cacheKeyTable, cacheKey, rNode.getName());
                }
            }
        }

        lock.lock();
        try {
            if (!writeToClient.get()) {
                if (prepared) {
                    if (rowDataPk == null) {
                        rowDataPk = new RowDataPacket(fieldCount);
                        rowDataPk.read(row);
                    }
                    BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                    binRowDataPk.read(fieldPackets, rowDataPk);
                    binRowDataPk.setPacketId(rowDataPk.getPacketId());
                    buffer = binRowDataPk.write(buffer, session.getSource(), true);
                } else {
                    buffer = session.getSource().writeToBuffer(row, buffer);
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (connClosed) {
            return;
        }
        connClosed = true;
        LOGGER.warn("Backend connect Closed, reason is [" + reason + "], Connection info:" + conn);
        reason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(++packetId);
        err.setErrNo(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        this.backConnectionErr(err, conn, true);
    }

    @Override
    public void requestDataResponse(byte[] data, BackendConnection conn) {
        LoadDataUtil.requestFileDataResponse(data, conn);
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    @Override
    public String toString() {
        return "SingleNodeHandler [node=" + node + ", packetId=" + packetId + "]";
    }

    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            session.clearResources(true);
            recycleBuffer();
            return true;
        } else {
            return false;
        }
    }
}
