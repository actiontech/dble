/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.route.LoadDataRouteResultsetNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.RequestScope;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.status.LoadDataBatch;
import com.actiontech.dble.server.variables.OutputStateEnum;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.DebugUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;

import static com.actiontech.dble.net.mysql.StatusFlags.SERVER_STATUS_CURSOR_EXISTS;

/**
 * @author ylz
 */
public class MultiNodeLoadDataHandler extends MultiNodeHandler implements LoadDataResponseHandler, ExecutableHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeLoadDataHandler.class);
    protected final RouteResultset rrs;
    protected final boolean sessionAutocommit;
    private long affectedRows;
    long selectRows;
    protected List<MySQLResponseService> errConnection;
    private LongAdder netOutBytes = new LongAdder();
    private LongAdder resultSize = new LongAdder();
    protected ErrorPacket err;
    protected int fieldCount = 0;
    volatile boolean fieldsReturned;
    private long insertId;
    protected volatile ByteBuffer byteBuffer;
    protected Set<MySQLResponseService> closedConnSet;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    private final boolean modifiedSQL;
    protected Set<LoadDataRouteResultsetNode> connRrns = new ConcurrentSkipListSet<>();
    private Map<String, Integer> shardingNodePauseInfo; // only for debug
    private int errorCount;
    private OkPacket packet;
    private Set<String> dnSet = new ConcurrentSkipListSet<>();
    private RequestScope requestScope;
    private int errorNodeCount;

    public MultiNodeLoadDataHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multi node query " + rrs.getStatement());
        }
        this.rrs = rrs;
        this.sessionAutocommit = session.getShardingService().isAutocommit();
        this.modifiedSQL = rrs.getNodes()[0].isModifySQL();
        requestScope = session.getShardingService().getRequestScope();
        TxnLogHelper.putTxnLog(session.getShardingService(), rrs);
        initDebugInfo();
    }

    @Override
    protected void reset() {
        super.reset();
        connRrns.clear();
        this.netOutBytes.reset();
        this.resultSize.reset();
        dnSet.clear();
        packet = null;
        errorCount = 0;
        errorNodeCount = 0;
        unResponseRrns.clear();
        LoadDataBatch.getInstance().clean();
        deleteErrorFile();
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void handlerCommit(RouteResultsetNode rrn) {
        session.getTarget(rrn).getBackendService().commit();
    }

    @Override
    public void execute() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-for-sql");
        try {
            lock.lock();
            try {
                this.reset();
                this.fieldsReturned = false;
                this.affectedRows = 0L;
                this.insertId = 0L;
            } finally {
                lock.unlock();
            }
            LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
            for (RouteResultsetNode node : rrs.getNodes()) {
                unResponseRrns.add(node);
            }
            Map<String, List<LoadDataRouteResultsetNode>> multiRouteResultSetNodeMap = rrs.getMultiRouteResultSetNodeMap();
            for (Map.Entry<String, List<LoadDataRouteResultsetNode>> entry : multiRouteResultSetNodeMap.entrySet()) {
                executeNodeList(entry.getValue());
            }
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    @Override
    public void clearAfterFailExecute() {
        if (session.getShardingService().isInTransaction()) {
            session.getShardingService().setTxInterrupt("ROLLBACK");
        }
        cleanBuffer();
        session.forceClose("other node prepare conns failed");
    }

    private void executeNodeList(List<LoadDataRouteResultsetNode> routeResultsetNodeList) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
            Set<String> oldFileNames = LoadDataBatch.getInstance().getSuccessFileNames();
            for (LoadDataRouteResultsetNode node : routeResultsetNodeList) {
                String fileName = node.getLoadData().getFileName();
                if (!dnSet.contains(node.getName()) && !oldFileNames.contains(fileName)) {
                    try {
                        connection(node);
                    } catch (Exception e) {
                        if (!dnSet.contains(node.getName())) {
                            setFail(e.toString());
                            dnSet.add(node.getName());
                        }
                        unResponseRrns.remove(node);
                        if (unResponseRrns.isEmpty()) {
                            handleDataProcessException(e);
                        }
                    }
                } else {
                    unResponseRrns.remove(node);
                    if (unResponseRrns.isEmpty() && !Strings.isNullOrEmpty(this.error)) {
                        handleDataProcessException(new Exception(this.error));
                    }
                }
            }
        });
    }

    private void connection(LoadDataRouteResultsetNode node) throws Exception {
        connRrns.add(node);
        // create new connection
        node.setRunOnSlave(rrs.getRunOnSlave());
        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("[doConnection] node + " + node.toString());
        dn.syncGetConnection(dn.getDatabase(), session.getShardingService().isTxStart(), sessionAutocommit, node, this, node);
    }


    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().executeMultiNodeForLoadData(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart() && !node.isModifySQL());
    }


    @Override
    public void writeRemainBuffer() {
        lock.lock();
        try {
            if (byteBuffer != null) {
                session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
                byteBuffer = null;
            }
        } finally {
            lock.unlock();
        }
    }

    void cleanBuffer() {
        lock.lock();
        try {
            if (byteBuffer != null) {
                session.getSource().recycle(byteBuffer);
                byteBuffer = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        pauseTime((MySQLResponseService) service);
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-connection-closed");
        TraceManager.finishSpan(service, traceObject);
        if (checkClosedConn((MySQLResponseService) service)) {
            return;
        }
        LOGGER.warn("backend connect " + reason + ", conn info:" + service);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        errPacket.setMessage(StringUtil.encode(reason, session.getShardingService().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            session.getSource().setSkipCheck(false);
            RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            dnSet.add(rNode.getName());
            removeNode(rNode.getName());
            rNode.setLoadDataRrnStatus((byte) 1);
            session.getTargetMap().remove(rNode);
            ((MySQLResponseService) service).setResponseHandler(null);
            executeError((MySQLResponseService) service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        LOGGER.warn(errMsg);
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            removeNode(rrn.getName());
            dnSet.add(rrn.getName());
            rrn.setLoadDataRrnStatus((byte) 1);
            executeError(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection connection) {
        final RouteResultsetNode node = (RouteResultsetNode) connection.getBackendService().getAttachment();
        session.bindConnection(node, connection);
        connRrns.remove(node);
        innerExecute(connection, node);
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-sql-execute-error");
        TraceManager.finishSpan(service, traceObject);
        pauseTime((MySQLResponseService) service);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            RouteResultsetNode rrn = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            dnSet.add(rrn.getName());
            removeNode(rrn.getName());
            rrn.setLoadDataRrnStatus((byte) 1);
            if (!isFail()) {
                err = errPacket;
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add((MySQLResponseService) service);
            if (decrementToZero((MySQLResponseService) service)) {
                if (session.closed()) {
                    cleanBuffer();
                } else if (byteBuffer != null) {
                    session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
                }
                //just for normal error
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            }
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    public void specialOkResponse(AbstractService service) {
        ShardingService shardingService = session.getShardingService();
        lock.lock();
        try {
            errorNodeCount++;
            decrementToZero((MySQLResponseService) service);
            if (unResponseRrns.size() != 0 || dnSet.size() > errorNodeCount) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("errorCount " + errorCount + " warningSize =  " + LoadDataBatch.getInstance().getWarnings().size());
                }
                return;
            }
            if (rrs.isGlobalTable()) {
                affectedRows = affectedRows / LoadDataBatch.getInstance().getCurrentNodeSize();
            }
            shardingService.getLoadDataInfileHandler().clearFile(LoadDataBatch.getInstance().getSuccessFileNames());
            if (isFail()) {
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
                return;
            }
            packet.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings:" + errorCount + "\r\n" +
                    toStringForWarning(LoadDataBatch.getInstance().getWarnings())).getBytes());
            session.setRowCount(affectedRows);
            shardingService.setLastInsertId(packet.getInsertId());
            handleEndPacket(packet, AutoTxOperation.ROLLBACK, true);
            cleanBuffer();
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    private String toStringForWarning(Map<String, List<String>> warns) {
        StringBuilder sb = new StringBuilder("node   reason");
        String blackSpace = "  ";
        String line = "\r\n";
        warns.forEach((k, v) -> {
            if (rrs.isGlobalTable()) {
                Set<String> warnSet = new HashSet<>(v);
                warnSet.forEach(warn -> sb.append(line).append(k).append(blackSpace).append(warn));
            } else {
                v.forEach(warn -> sb.append(line).append(k).append(blackSpace).append(warn));
            }
        });
        return sb.toString();
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-response");
        TraceManager.finishSpan(service, traceObject);
        this.netOutBytes.add(data.length);
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return;
        }
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }
        if (executeResponse) {
            pauseTime((MySQLResponseService) service);
            this.resultSize.add(data.length);
            session.trace(t -> t.setBackendResponseEndTime((MySQLResponseService) service));
            ShardingService shardingService = session.getShardingService();
            OkPacket ok = new OkPacket();
            ok.read(data);
            lock.lock();
            try {
                if (ok.getAffectedRows() > 0 || ok.getWarningCount() > 0) {
                    RouteResultsetNode rrn = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
                    // the affected rows of global table will use the last node's response
                    affectedRows += ok.getAffectedRows();
                    if (ok.getInsertId() > 0) {
                        insertId = (insertId == 0) ? ok.getInsertId() : Math.min(insertId, ok.getInsertId());
                    }
                    if (ok.getWarningCount() > 0) {
                        affectedRows -= ok.getAffectedRows();
                        if (rrs.isGlobalTable()) {
                            errorCount = ok.getWarningCount();
                        } else {
                            errorCount += ok.getWarningCount();
                        }
                        dnSet.add(rrn.getName());
                        transformOkPackage(ok, shardingService);
                        if (packet == null)
                            packet = ok;
                        createErrorFile(rrn.getLoadData().getData(), rrn, FileUtils.getName(rrn.getLoadData().getFileName()));
                        rrn.setLoadDataRrnStatus((byte) 2);
                        return;
                    }
                    String filePath = rrn.getLoadData().getFileName();
                    LoadDataBatch.getInstance().setFileName(filePath);
                    handlerCommit(rrn);
                    FileUtils.deleteFile(filePath);
                    rrn.setLoadDataRrnStatus((byte) 1);
                    decrementToZero((MySQLResponseService) service);
                    if (unResponseRrns.size() != 0) {
                        return;
                    }
                    if (rrs.isGlobalTable()) {
                        affectedRows = affectedRows / LoadDataBatch.getInstance().getCurrentNodeSize();
                    }
                    if (errorCount > 0) {
                        specialOkResponse(service);
                    } else {
                        if (isFail()) {
                            ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                            handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
                            return;
                        }
                        LoadDataBatch.getInstance().getSuccessFileNames().clear();
                        ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0").getBytes());
                        shardingService.getLoadDataInfileHandler().clear();
                        shardingService.getLoadDataInfileHandler().cleanLoadDataFile();
                        transformOkPackage(ok, shardingService);
                        session.trace(t -> t.doSqlStat(ok.getAffectedRows(), netOutBytes.intValue(), resultSize.intValue()));
                        deleteErrorFile();
                        handleEndPacket(ok, AutoTxOperation.COMMIT, true);
                        cleanBuffer();
                    }
                }
            } catch (Exception e) {
                cleanBuffer();
                handleDataProcessException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketList, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        this.netOutBytes.add(header.length);
        for (byte[] field : fields) {
            this.netOutBytes.add(field.length);
        }
        this.netOutBytes.add(eof.length);
        if (fieldsReturned) {
            return;
        }
        lock.lock();
        try {
            if (session.closed()) {
                cleanBuffer();
                return;
            }
            if (fieldsReturned) {
                return;
            }

            if (byteBuffer == null && ServerParse.LOAD_DATA_INFILE_SQL == rrs.getSqlType()) {
                byteBuffer = session.getSource().allocate();
            }
            this.resultSize.add(header.length);
            for (byte[] field : fields) {
                this.resultSize.add(field.length);
            }
            this.resultSize.add(eof.length);
            fieldsReturned = true;
            executeFieldEof(header, fields, eof);
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        if (byteBuffer == null) {
            return;
        }
        ShardingService service = session.getShardingService();
        fieldCount = fields.size();
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return;
        }
        header[3] = (byte) service.nextPacketId();
        byteBuffer = service.writeToBuffer(header, byteBuffer);

        if (!errorResponse.get()) {
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
                fieldPkg.setPacketId(session.getShardingService().nextPacketId());
                byteBuffer = fieldPkg.write(byteBuffer, service, false);
            }
            if (requestScope.isUsingCursor()) {
                requestScope.getCurrentPreparedStatement().initCursor(session, this, fields.size(), fieldPackets);
            }

            eof[3] = (byte) session.getShardingService().nextPacketId();
            if (requestScope.isUsingCursor()) {
                byte statusFlag = 0;
                statusFlag |= service.getSession2().getShardingService().isAutocommit() ? 2 : 1;
                statusFlag |= SERVER_STATUS_CURSOR_EXISTS;
                eof[7] = statusFlag;
            }
            byteBuffer = service.writeToBuffer(eof, byteBuffer);
        }
    }


    private void createErrorFile(List<String> data, RouteResultsetNode rrn, String destFileName) {
        String temp = SystemConfig.getInstance().getHomePath() + File.separator + "temp" + File.separator + "error" + File.separator;
        String srcFileName = rrn.getLoadData().getFileName();
        String resFilePath = temp + destFileName;
        StringBuilder sb = new StringBuilder();
        File resFile = new File(resFilePath);
        try {
            Files.createParentDirs(resFile);
            if (rrs.isGlobalTable()) {
                if (Strings.isNullOrEmpty(srcFileName)) {
                    data.forEach(row -> sb.append(row).append("\r\n"));
                    Files.write(sb.toString().getBytes(), resFile);
                } else {
                    FileUtils.copy(new File(srcFileName), resFile);
                }
            } else {
                if (!Strings.isNullOrEmpty(srcFileName)) {
                    data = Files.readLines(new File(srcFileName), Charset.defaultCharset());
                }
                data.forEach(row -> sb.append(row).append("\r\n"));
                FileUtils.write(resFilePath, sb.toString());
            }
        } catch (IOException e) {
            LOGGER.warn("file write error", e);
        }
    }

    private void deleteErrorFile() {
        String temp = SystemConfig.getInstance().getHomePath() + File.separator + "temp" + File.separator + "error" + File.separator;
        FileUtils.deleteFile(temp);
    }

    private void transformOkPackage(OkPacket ok, ShardingService shardingService) {
        ok.setPacketId(session.getShardingService().nextPacketId());
        ok.setAffectedRows(affectedRows);
        session.setRowCount(affectedRows);
        ok.setServerStatus(shardingService.isAutocommit() ? 2 : 1);
        if (insertId > 0) {
            ok.setInsertId(insertId);
            shardingService.setLastInsertId(insertId);
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        specialOkResponse(service);
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            if (session.closed()) {
                cleanBuffer();
            }
            RouteResultsetNode rrn = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            RowDataPacket rowDataPkg = new RowDataPacket(3);
            rowDataPkg.read(row);
            removeNode(rrn.getName());
            Map<String, List<String>> warnings = LoadDataBatch.getInstance().getWarnings();
            String name = rrn.getName();
            if (!warnings.containsKey(name)) {
                warnings.put(name, Lists.newArrayList());
            }
            warnings.get(name).add(new String(rowDataPkg.fieldValues.get(2)));
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public void clearResources() {
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
        cleanBuffer();
    }

    @Override
    public void requestDataResponse(byte[] data, @Nonnull MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }

    private void executeError(@Nullable MySQLResponseService service) {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (errConnection == null) {
            errConnection = new ArrayList<>();
        }
        if (service != null && !service.isFakeClosed()) {
            errConnection.add(service);
            if (service.getConnection().isClosed() && session.getShardingService().isInTransaction()) {
                session.getShardingService().setTxInterrupt(error);
            }
        }
        if (unResponseRrns.isEmpty()) {
            session.getShardingService().getLoadDataInfileHandler().clear();
            if (byteBuffer == null) {
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            } else if (session.closed()) {
                cleanBuffer();
            } else {
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            }
        }
    }

    void handleDataProcessException(Exception e) {
        if (!errorResponse.get()) {
            this.error = e.toString();
            LOGGER.info("caught exception ", e);
            this.tryErrorFinished(true);
        }
    }

    private boolean checkClosedConn(MySQLResponseService service) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(service);
            } else {
                if (closedConnSet.contains(service)) {
                    return true;
                }
                closedConnSet.add(service);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    void handleEndPacket(MySQLPacket curPacket, AutoTxOperation txOperation, boolean isSuccess) {
        ShardingService service = session.getShardingService();
        service.getLoadDataInfileHandler().clear();

        if (!service.isInTransaction() && this.modifiedSQL && !this.session.isKilled()) {
            //Implicit Distributed Transaction,send commit or rollback automatically
            TransactionHandler handler = new AutoCommitHandler(session, curPacket, rrs.getNodes(), errConnection);
            if (txOperation == AutoTxOperation.COMMIT) {
                session.checkBackupStatus();
                session.trace(t -> t.setBeginCommitTime());
                handler.commit();
            } else {
                service.getLoadDataInfileHandler().clearFile(LoadDataBatch.getInstance().getSuccessFileNames());
                handler.rollback();
            }
        } else {
            boolean inTransaction = service.isInTransaction();
            if (!inTransaction) {
                if (errConnection != null) {
                    for (MySQLResponseService servicex : errConnection) {
                        session.releaseConnection(servicex.getConnection());
                    }
                }
            }

            // Explicit Distributed Transaction
            if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                service.setTxInterrupt("ROLLBACK");
            }

            curPacket.write(session.getSource());
        }
    }

    private void initDebugInfo() {
        if (LOGGER.isDebugEnabled()) {
            String info = DebugUtil.getDebugInfo("MultiNodeQueryHandler.txt");
            if (info != null) {
                LOGGER.debug("Pause info of MultiNodeQueryHandler is " + info);
                String[] infos = info.split(";");
                shardingNodePauseInfo = new HashMap<>(infos.length);
                boolean formatCorrect = true;
                for (String nodeInfo : infos) {
                    try {
                        String[] infoDetail = nodeInfo.split(":");
                        shardingNodePauseInfo.put(infoDetail[0], Integer.valueOf(infoDetail[1]));
                    } catch (Throwable e) {
                        formatCorrect = false;
                        break;
                    }
                }
                if (!formatCorrect) {
                    shardingNodePauseInfo.clear();
                }
            } else {
                shardingNodePauseInfo = new HashMap<>(0);
            }
        } else {
            shardingNodePauseInfo = new HashMap<>(0);
        }
    }

    private void pauseTime(MySQLResponseService service) {
        if (LOGGER.isDebugEnabled()) {
            RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
            Integer millis = shardingNodePauseInfo.get(rNode.getName());
            if (millis == null) {
                return;
            }
            LOGGER.debug("shardingnode[" + rNode.getName() + "], which conn threadid[" + service.getConnection().getThreadId() + "] will sleep for " + millis + " milliseconds");
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.debug("shardingnode[" + rNode.getName() + "], which conn threadid[" + service.getConnection().getThreadId() + "] has slept for " + millis + " milliseconds");
        }
    }

    private void removeNode(String nodeName) {
        unResponseRrns.removeIf(node -> StringUtil.equals(node.getName(), nodeName));
    }
}
