package com.actiontech.dble.rwsplit;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.SessionStage;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.trace.RwTraceResult;
import com.actiontech.dble.server.trace.TraceRecord;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.rwsplit.*;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RWSplitNonBlockingSession {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;
    private volatile RwTraceResult traceResult = new RwTraceResult();

    private volatile SessionStage sessionStage = SessionStage.Init;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    public void execute(Boolean master, Callback callback) {
        execute(master, null, callback);
    }

    public void execute(Boolean master, Callback callback, boolean writeStatistical) {
        execute(master, null, callback, writeStatistical, false);
    }

    /**
     * @param master
     * @param callback
     * @param writeStatistical
     * @param localRead        only the SELECT and show statements attempt to localRead
     */
    public void execute(Boolean master, Callback callback, boolean writeStatistical, boolean localRead) {
        execute(master, null, callback, writeStatistical, localRead && rwGroup.usedForRW());
    }


    public void execute(Boolean master, byte[] originPacket, Callback callback) {
        execute(master, originPacket, callback, false, false);
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback, boolean writeStatistical) {
        execute(master, originPacket, callback, writeStatistical, false);
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback, boolean writeStatistical, boolean localRead) {
        try {
            RWSplitHandler handler = getRwSplitHandler(originPacket, callback);
            if (handler == null) return;
            PhysicalDbInstance instance = rwGroup.rwSelect(canRunOnMaster(master), isWriteStatistical(writeStatistical), localRead);
            checkDest(!instance.isReadInstance());
            endRoute();
            setPreExecuteEnd(RwTraceResult.SqlTraceType.RWSPLIT_QUERY);
            setTraceSimpleHandler((ResponseHandler) handler);
            traceResult.setDBInstance(instance);
            instance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (IOException e) {
            LOGGER.warn("select conn error", e);
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        } catch (SQLSyntaxErrorException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }

    @Nullable
    private RWSplitHandler getRwSplitHandler(byte[] originPacket, Callback callback) throws SQLSyntaxErrorException, IOException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback, false);
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            endRoute();
            setPreExecuteEnd(RwTraceResult.SqlTraceType.RWSPLIT_QUERY);
            setTraceSimpleHandler(handler);
            traceResult.setDBInstance((PhysicalDbInstance) conn.getInstance());
            // for ps needs to send master
            if ((originPacket != null && originPacket.length > 4 && originPacket[4] == MySQLPacket.COM_STMT_EXECUTE)) {
                long statementId = ByteUtil.readUB4(originPacket, 5);
                PreparedStatementHolder holder = rwSplitService.getPrepareStatement(statementId);
                if (holder.isMustMaster() && conn.getInstance().isReadInstance()) {
                    holder.setExecuteOrigin(originPacket);
                    PSHandler psHandler = new PSHandler(rwSplitService, holder);
                    psHandler.execute(rwGroup);
                    return null;
                }
            }
            checkDest(!conn.getInstance().isReadInstance());
            handler.execute(conn);
            return null;
        }
        return handler;
    }


    private Boolean canRunOnMaster(Boolean master) {
        if (!rwSplitService.isAutocommit() || rwSplitService.isTxStart()) {
            return true;
        }
        return master;
    }

    private boolean isWriteStatistical(boolean writeStatistical) {
        if (!rwSplitService.isAutocommit() || rwSplitService.isTxStart()) {
            return true;
        }
        return writeStatistical;
    }

    private void checkDest(boolean isMaster) throws SQLSyntaxErrorException {
        String dest = rwSplitService.getExpectedDest();
        if (dest == null) {
            return;
        }
        if (dest.equalsIgnoreCase("M") && isMaster) {
            return;
        }
        if (dest.equalsIgnoreCase("S") && !isMaster) {
            return;
        }
        throw new SQLSyntaxErrorException("unexpected dble_dest_expect,real[" + (isMaster ? "M" : "S") + "],expect[" + dest + "]");
    }

    public PhysicalDbGroup getRwGroup() {
        return rwGroup;
    }

    public void executeHint(int sqlType, String sql, Callback callback) throws IOException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, null, callback, true);
        try {
            PhysicalDbInstance dbInstance = RouteService.getInstance().routeRwSplit(sqlType, sql, rwSplitService);
            if (dbInstance == null) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("route sql {} to {}", sql, dbInstance);
            }
            endRoute();
            setPreExecuteEnd(RwTraceResult.SqlTraceType.RWSPLIT_QUERY);
            setTraceSimpleHandler((ResponseHandler) handler);
            traceResult.setDBInstance(dbInstance);
            dbInstance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }
    }

    private void executeException(Exception e, String sql) {
        sql = sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql;
        if (e instanceof SQLException) {
            SQLException sqlException = (SQLException) e;
            String msg = sqlException.getMessage();
            StringBuilder s = new StringBuilder();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(s.append(this).append(sql).toString() + " err:" + msg);
            }
            int vendorCode = sqlException.getErrorCode() == 0 ? ErrorCode.ER_PARSE_ERROR : sqlException.getErrorCode();
            String sqlState = StringUtil.isEmpty(sqlException.getSQLState()) ? "HY000" : sqlException.getSQLState();
            String errorMsg = msg == null ? sqlException.getClass().getSimpleName() : msg;
            rwSplitService.writeErrMessage(sqlState, errorMsg, vendorCode);
        } else {
            StringBuilder s = new StringBuilder();
            LOGGER.info(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            rwSplitService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        }
    }

    public void setRwGroup(PhysicalDbGroup rwGroup) {
        this.rwGroup = rwGroup;
    }

    public void bind(BackendConnection bindConn) {
        if (conn != null && conn != bindConn) {
            LOGGER.warn("last conn is remaining");
        }
        this.conn = bindConn;
    }

    public void unbindIfSafe() {
        if (rwSplitService.isAutocommit() && !rwSplitService.isTxStart() && !rwSplitService.isLocked() &&
                !rwSplitService.isInLoadData() &&
                !rwSplitService.isInPrepare() && this.conn != null && rwSplitService.getPsHolder().isEmpty()) {
            this.conn.release();
            this.conn = null;
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public void close(String reason) {
        if (conn != null) {
            conn.close(reason);
        }
    }

    public RWSplitService getService() {
        return rwSplitService;
    }

    public BackendConnection getConn() {
        return conn;
    }


    public void setRequestTime() {
        sessionStage = SessionStage.Read_SQL;
        long requestTime = 0;

        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            requestTime = System.nanoTime();
            traceResult.setVeryStartPrepare(requestTime);
        }

    }

    public void startProcess() {
        sessionStage = SessionStage.Parse_SQL;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setParseStartPrepare(new TraceRecord(System.nanoTime()));
        }
    }

    public void endParse() {
        sessionStage = SessionStage.Route_Calculation;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.ready();
            //            traceResult.setRouteStart(new TraceRecord(System.nanoTime()));
        }
    }


    public void endRoute() {
        sessionStage = SessionStage.Prepare_to_Push;
    }


    public void setPreExecuteEnd(RwTraceResult.SqlTraceType type) {
        sessionStage = SessionStage.Execute_SQL;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setType(type);
            traceResult.setPreExecuteEnd(new TraceRecord(System.nanoTime()));
            traceResult.clearConnReceivedMap();
            traceResult.clearConnFlagMap();
        }
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setSimpleHandler(simpleHandler);
        }
    }


    public void setResponseTime(boolean isSuccess) {
        sessionStage = SessionStage.Finished;
        long responseTime = 0;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            responseTime = System.nanoTime();
            traceResult.setVeryEnd(responseTime);
            if (isSuccess && SlowQueryLog.getInstance().isEnableSlowLog()) {
                SlowQueryLog.getInstance().putSlowQueryLog(this.rwSplitService, (RwTraceResult) traceResult);
                traceResult = new RwTraceResult();
            }
        }
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
        sessionStage = SessionStage.First_Node_Fetched_Result;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            ResponseHandler responseHandler = service.getResponseHandler();
            if (responseHandler != null) {
                TraceRecord record = new TraceRecord(System.nanoTime());
                Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
                String key = String.valueOf(service.getConnection().getId());
                connMap.put(key, record);
                traceResult.addToConnFinishedMap(responseHandler, connMap);
            }
        }

    }



    public void setBackendResponseTime(MySQLResponseService service) {
        sessionStage = SessionStage.Fetching_Result;
        long responseTime = 0;
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            ResponseHandler responseHandler = service.getResponseHandler();
            String key = String.valueOf(service.getConnection().getId());
            if (responseHandler != null && traceResult.addToConnFlagMap(key) == null) {
                responseTime = System.nanoTime();
                TraceRecord record = new TraceRecord(responseTime);
                Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.put(key, record);
                traceResult.addToConnReceivedMap(responseHandler, connMap);
            }
        }


    }



}
