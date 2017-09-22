/*
* Copyright (C) 2016-2017 ActionTech.
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
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.memory.unsafe.row.UnsafeRow;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.sqlengine.mpp.*;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

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
    private List<FieldPacket> fieldPackets = new ArrayList<>();
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
        int isOffHeapuseOffHeapForMerge = DbleServer.getInstance().
                getConfig().getSystem().getUseOffHeapForMerge();
        if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
            if (isOffHeapuseOffHeapForMerge == 1) {
                dataMergeSvr = new DataNodeMergeManager(this, rrs);
            } else {
                dataMergeSvr = new DataMergeService(this, rrs);
            }
        } else {
            dataMergeSvr = null;
        }

        isCallProcedure = rrs.isCallStatement();
        this.sessionAutocommit = session.getSource().isAutocommit();
        this.session = session;
        this.lock = new ReentrantLock();
        if ((dataMergeSvr != null) && LOGGER.isDebugEnabled()) {
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
        lock.lock();
        try {
            this.reset(rrs.getNodes().length);
            this.fieldsReturned = false;
            this.affectedRows = 0L;
            this.insertId = 0L;
        } finally {
            lock.unlock();
        }
        ServerConfig conf = DbleServer.getInstance().getConfig();
        startTime = System.currentTimeMillis();
        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            if (node.isModifySQL()) {
                sb.append("[" + node.getName() + "]" + node.getStatement()).append(";\n");
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
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        conn.execute(node, session.getSource(), sessionAutocommit && !session.getSource().isTxstart() && !node.isModifySQL());
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

        LOGGER.warn(s.toString());

        return;
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrno(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;
        lock.lock();
        try {
            if (!terminated) {
                terminated = true;
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (--nodeCount <= 0) {
                handleDdl();
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
        errPacket.setPacketId(++packetId);
        errPacket.setErrno(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getSource().getCharset().getResults()));
        err = errPacket;
        lock.lock();
        try {
            if (!terminated) {
                terminated = true;
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (--nodeCount <= 0) {
                handleDdl();
                session.handleSpecial(rrs, session.getSource().getSchema(), false);
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
            }
        } finally {
            lock.unlock();
        }
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
            if (!isFail())
                setFail(err.toString());
            if (--nodeCount > 0)
                return;
            handleDdl();
            session.handleSpecial(rrs, session.getSource().getSchema(), false);
            handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
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
                if (isFail() || terminated) {
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
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end reseponse " + conn);
        }

        this.netOutBytes += eof.length;

        if (errorResponsed.get()) {
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
            if (!rrs.isCallStatement() || (rrs.isCallStatement() && rrs.getProcedure().isResultSimpleValue())) {
                if (this.sessionAutocommit && !session.getSource().isTxstart() && !session.getSource().isLocked()) { // clear all connections
                    session.releaseConnections(false);
                }

                if (this.isFail() || session.closed()) {
                    tryErrorFinished(true);
                    return;
                }
            }
            writeEofResult(eof, source);
            doSqlStat(source);
        }

    }

    private void writeEofResult(byte[] eof, ServerConnection source) {
        if (dataMergeSvr != null) {
            try {
                dataMergeSvr.outputMergeResult(session, eof);
            } catch (Exception e) {
                handleDataProcessException(e);
            }

        } else {
            lock.lock();
            try {
                eof[3] = ++packetId;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("last packet id:" + packetId);
                }
                source.write(eof);
            } finally {
                lock.unlock();
            }
        }
    }

    private void doSqlStat(ServerConnection source) {
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

    /**
     * send the final result to the client
     *
     * @param source
     * @param eof
     * @param
     */
    public void outputMergeResult(final ServerConnection source, final byte[] eof, Iterator<UnsafeRow> iter) {
        lock.lock();
        try {
            ByteBuffer buffer = session.getSource().allocate();
            final RouteResultset routeResultset = this.dataMergeSvr.getRrs();

            /**
             * cut the result for the limit statement
             */
            int start = routeResultset.getLimitStart();
            int end = start + routeResultset.getLimitSize();
            int index = 0;

            if (start < 0)
                start = 0;

            if (routeResultset.getLimitSize() < 0)
                end = Integer.MAX_VALUE;

            if (prepared) {
                while (iter.hasNext()) {
                    UnsafeRow row = iter.next();
                    if (index >= start) {
                        row.setPacketId(++packetId);
                        BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
                        binRowPacket.read(fieldPackets, row);
                        buffer = binRowPacket.write(buffer, source, true);
                    }
                    index++;
                    if (index == end) {
                        break;
                    }
                }
            } else {
                while (iter.hasNext()) {
                    UnsafeRow row = iter.next();
                    if (index >= start) {
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, source, true);
                    }
                    index++;
                    if (index == end) {
                        break;
                    }
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
            try {
                dataMergeSvr.clear();
            } finally {
                lock.unlock();
            }
        }
    }

    public void outputMergeResult(final ServerConnection source,
                                  final byte[] eof, List<RowDataPacket> results) {
        lock.lock();
        try {
            ByteBuffer buffer = session.getSource().allocate();
            final RouteResultset routeResultset = this.dataMergeSvr.getRrs();

            int start = routeResultset.getLimitStart();
            int end = start + routeResultset.getLimitSize();

            if (start < 0) {
                start = 0;
            }

            if (routeResultset.getLimitSize() < 0) {
                end = results.size();
            }

            if (end > results.size()) {
                end = results.size();
            }

            if (prepared) {
                for (int i = start; i < end; i++) {
                    RowDataPacket row = results.get(i);
                    BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                    binRowDataPk.read(fieldPackets, row);
                    binRowDataPk.setPacketId(++packetId);
                    //binRowDataPk.write(source);
                    buffer = binRowDataPk.write(buffer, session.getSource(), true);
                }
            } else {
                for (int i = start; i < end; i++) {
                    RowDataPacket row = results.get(i);
                    row.setPacketId(++packetId);
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
            try {
                dataMergeSvr.clear();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsnull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        this.netOutBytes += eof.length;
        for (byte[] field : fields) {
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
            if (dataMergeSvr != null) {
                mergeFieldEof(fields, eof);
            } else {
                executeFieldEof(header, fields, eof);
            }
        } catch (Exception e) {
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        ServerConnection source = session.getSource();
        ByteBuffer buffer = source.allocate();
        fieldCount = fields.size();
        header[3] = ++packetId;
        buffer = source.writeToBuffer(header, buffer);
        String primaryKey = null;
        if (rrs.hasPrimaryKeyToCache()) {
            String[] items = rrs.getPrimaryKeyItems();
            priamaryKeyTable = items[0];
            primaryKey = items[1];
        }

        for (int i = 0, len = fieldCount; i < len; ++i) {
            byte[] field = fields.get(i);
            FieldPacket fieldPkg = new FieldPacket();
            fieldPkg.read(field);
            fieldPackets.add(fieldPkg);
            fieldCount = fields.size();
            if (primaryKey != null && primaryKeyIndex == -1) {
                // find primary key index
                String fieldName = new String(fieldPkg.getName());
                if (primaryKey.equalsIgnoreCase(fieldName)) {
                    primaryKeyIndex = i;
                }
            }
            field[3] = ++packetId;
            buffer = source.writeToBuffer(field, buffer);
        }
        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
        source.write(buffer);
    }

    private void mergeFieldEof(List<byte[]> fields, byte[] eof) throws IOException {
        Set<String> shouldRemoveAvgField = new HashSet<>();
        Set<String> shouldRenameAvgField = new HashSet<>();
        Map<String, Integer> mergeColsMap = dataMergeSvr.getRrs().getMergeCols();
        if (mergeColsMap != null) {
            for (Map.Entry<String, Integer> entry : mergeColsMap.entrySet()) {
                String key = entry.getKey();
                int mergeType = entry.getValue();
                if (MergeCol.MERGE_AVG == mergeType && mergeColsMap.containsKey(key + "SUM")) {
                    shouldRemoveAvgField.add((key + "COUNT").toUpperCase());
                    shouldRenameAvgField.add((key + "SUM").toUpperCase());
                }
            }
        }
        fieldCount = fields.size();
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.setPacketId(++packetId);
        packet.setFieldCount(fieldCount - shouldRemoveAvgField.size());
        ServerConnection source = session.getSource();
        ByteBuffer buffer = source.allocate();
        buffer = packet.write(buffer, source, true);

        Map<String, ColMeta> columToIndx = new HashMap<>(fieldCount);
        for (int i = 0, len = fieldCount; i < len; ++i) {
            boolean shouldSkip = false;
            byte[] field = fields.get(i);
            FieldPacket fieldPkg = new FieldPacket();
            fieldPkg.read(field);
            fieldPackets.add(fieldPkg);
            String fieldName = new String(fieldPkg.getName()).toUpperCase();
            if (columToIndx != null && !columToIndx.containsKey(fieldName)) {
                if (shouldRemoveAvgField.contains(fieldName)) {
                    shouldSkip = true;
                    fieldPackets.remove(fieldPackets.size() - 1);
                }
                if (shouldRenameAvgField.contains(fieldName)) {
                    String newFieldName = fieldName.substring(0,
                            fieldName.length() - 3);
                    fieldPkg.setName(newFieldName.getBytes());
                    fieldPkg.setPacketId(++packetId);
                    shouldSkip = true;
                    // Number of bits and precision of AVG. AVG bits = SUM bits - 14
                    fieldPkg.setLength(fieldPkg.getLength() - 14);
                    // AVG precision = SUM precision + 4
                    fieldPkg.setDecimals((byte) (fieldPkg.getDecimals() + 4));
                    buffer = fieldPkg.write(buffer, source, false);

                    // reset precision
                    fieldPkg.setDecimals((byte) (fieldPkg.getDecimals() - 4));
                }

                ColMeta colMeta = new ColMeta(i, fieldPkg.getType());
                colMeta.setDecimals(fieldPkg.getDecimals());
                columToIndx.put(fieldName, colMeta);
            }
            if (!shouldSkip) {
                field[3] = ++packetId;
                buffer = source.writeToBuffer(field, buffer);
            }
        }
        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
        source.write(buffer);
        dataMergeSvr.onRowMetaData(columToIndx, fieldCount);
    }

    public void handleDataProcessException(Exception e) {
        if (!errorResponsed.get()) {
            this.error = e.toString();
            LOGGER.warn("caught exception ", e);
            setFail(e.toString());
            this.tryErrorFinished(true);
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketnull, boolean isLeft, BackendConnection conn) {

        if (errorResponsed.get()) {
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
                RowDataPacket rowDataPkg = null;
                // cache primaryKey-> dataNode
                if (primaryKeyIndex != -1) {
                    rowDataPkg = new RowDataPacket(fieldCount);
                    rowDataPkg.read(row);
                    String primaryKey = new String(rowDataPkg.fieldValues.get(primaryKeyIndex));
                    LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
                    if (pool != null) {
                        pool.putIfAbsent(priamaryKeyTable, primaryKey, dataNode);
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

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    protected void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn) {
        ServerConnection source = session.getSource();
        if (source.isAutocommit() && !source.isTxstart() && conn.isModifiedSQLExecuted()) {
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
            boolean inTransaction = !source.isAutocommit() || source.isTxstart();
            if (!inTransaction) {
                session.releaseConnection(conn);
            }
            // Explicit Distributed Transaction
            if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                source.setTxInterrupt("ROLLBACK");
            }
            if (nodeCount == 0) {
                session.getSource().write(data);
            }
        }
    }
}
