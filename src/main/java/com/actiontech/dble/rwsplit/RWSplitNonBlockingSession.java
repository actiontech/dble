package com.actiontech.dble.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.RwSplitSelectVariablesHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.services.rwsplit.handle.PSHandler;
import com.actiontech.dble.services.rwsplit.handle.PreparedStatementHolder;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;

public class RWSplitNonBlockingSession extends Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;
    private int reSelectNum;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    @Override
    public FrontendConnection getSource() {
        return (FrontendConnection) rwSplitService.getConnection();
    }


    @Override
    public void stopFlowControl() {
        LOGGER.info("Session stop flow control " + this.getSource());
        synchronized (this) {
            rwSplitService.getConnection().setFlowControlled(false);
            final BackendConnection con = this.conn;
            if (con != null) {
                con.getSocketWR().enableRead();
            }
        }
    }

    @Override
    public void startFlowControl() {
        synchronized (this) {
            if (!rwSplitService.isFlowControlled()) {
                LOGGER.info("Session start flow control " + this.getSource());
            }
            rwSplitService.getConnection().setFlowControlled(true);
            this.conn.getSocketWR().disableRead();
        }
    }

    @Override
    public void releaseConnectionFromFlowControlled(BackendConnection con) {
        synchronized (this) {
            con.getSocketWR().enableRead();
            rwSplitService.getConnection().setFlowControlled(false);
        }
    }

    public void execute(Boolean master, Callback callback) {
        execute(master, null, callback);
    }

    public void execute(Boolean master, Callback callback, boolean write) {
        execute(master, null, callback, write);
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback) {
        try {
            RWSplitHandler handler = getRwSplitHandler(originPacket, callback);
            if (handler == null) return;
            getConnection(handler, master, null);
        } catch (SQLSyntaxErrorException | IOException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback, boolean write) {
        try {
            RWSplitHandler handler = getRwSplitHandler(originPacket, callback);
            if (handler == null) return;
            getConnection(handler, master, isWrite(write));
        } catch (SQLSyntaxErrorException | IOException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }


    /**
     * jdbc compatible pre-delivery statements
     * @param master
     * @param originPacket
     * @param callback
     * @param write
     */
    public void selectCompatibilityVariables(Boolean master, byte[] originPacket, Callback callback, boolean write) {
        try {
            RWSplitHandler handler = getRwSplitSelectVariablesHandler(originPacket, callback);
            if (handler == null) return;
            getConnection(handler, master, isWrite(write));
        } catch (SQLSyntaxErrorException | IOException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }

    public void getConnection(RWSplitHandler handler, Boolean master, Boolean write) {
        try {
            Boolean isMaster = canRunOnMaster(master);
            PhysicalDbInstance instance = reSelectRWDbGroup(rwGroup).rwSelect(isMaster, write);
            checkDest(!instance.isReadInstance());
            instance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (SQLSyntaxErrorException se) {
            rwGroup.unBindRwSplitSession(this);
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        } catch (IOException e) {
            LOGGER.warn("select conn error", e);
            rwGroup.unBindRwSplitSession(this);
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        } catch (Exception | Error e) {
            rwGroup.unBindRwSplitSession(this);
            throw e;
        }
    }

    @Nullable
    private RWSplitHandler getRwSplitHandler(byte[] originPacket, Callback callback) throws SQLSyntaxErrorException, IOException {
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            // for ps needs to send master
            if ((originPacket != null && originPacket.length > 4 && originPacket[4] == MySQLPacket.COM_STMT_EXECUTE)) {
                long statementId = ByteUtil.readUB4(originPacket, 5);
                PreparedStatementHolder holder = rwSplitService.getPrepareStatement(statementId);
                StatisticListener.getInstance().record(rwSplitService, r -> r.onFrontendSetSql(getService().getSchema(), holder.getPrepareSql()));
                if (holder.isMustMaster() && conn.getInstance().isReadInstance()) {
                    holder.setExecuteOrigin(originPacket);
                    PSHandler handler = new PSHandler(rwSplitService, holder);
                    handler.execute(rwGroup);
                    return null;
                }
            }
            RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback, false);
            checkDest(!conn.getInstance().isReadInstance());
            handler.execute(conn);
            return null;
        }
        return new RWSplitHandler(rwSplitService, originPacket, callback, false);
    }

    @Nullable
    private RWSplitHandler getRwSplitSelectVariablesHandler(byte[] originPacket, Callback callback) throws SQLSyntaxErrorException, IOException {
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            RWSplitHandler handler = new RwSplitSelectVariablesHandler(rwSplitService, originPacket, callback, false);
            checkDest(!conn.getInstance().isReadInstance());
            handler.execute(conn);
            return null;
        }
        return new RwSplitSelectVariablesHandler(rwSplitService, originPacket, callback, false);
    }

    private Boolean canRunOnMaster(Boolean master) {
        if (!rwSplitService.isAutocommit() || rwSplitService.isTxStart() || rwSplitService.isUsingTmpTable()) {
            return true;
        }
        return master;
    }

    private boolean isWrite(boolean write) {
        if (!rwSplitService.isAutocommit() || rwSplitService.isTxStart() || rwSplitService.isUsingTmpTable()) {
            return true;
        }
        return write;
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

    private PhysicalDbGroup reSelectRWDbGroup(PhysicalDbGroup dbGroup) {
        dbGroup.bindRwSplitSession(this);
        if (dbGroup.isStop()) {
            dbGroup.unBindRwSplitSession(this);
            if (reSelectNum == 10) {
                reSelectNum = 0;
                LOGGER.warn("dbGroup`{}` is always invalid", rwSplitService.getUserConfig().getDbGroup());
                throw new ConfigException("the dbGroup`" + rwSplitService.getUserConfig().getDbGroup() + "` is always invalid, pls check reason");
            }
            PhysicalDbGroup newDbGroup = DbleServer.getInstance().getConfig().getDbGroups().get(rwSplitService.getUserConfig().getDbGroup());
            if (newDbGroup == null) {
                LOGGER.warn("dbGroup`{}` is invalid", rwSplitService.getUserConfig().getDbGroup());
                throw new ConfigException("the dbGroup`" + rwSplitService.getUserConfig().getDbGroup() + "` is invalid");
            } else {
                reSelectNum++;
                return reSelectRWDbGroup(newDbGroup);
            }
        } else {
            reSelectNum = 0;
            this.rwGroup = dbGroup;
        }
        return dbGroup;
    }

    public PhysicalDbGroup getRwGroup() {
        return rwGroup;
    }

    public void executeHint(int sqlType, String sql, Callback callback) {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, null, callback, true);
        try {
            PhysicalDbInstance dbInstance = RouteService.getInstance().routeRwSplit(sqlType, sql, rwSplitService);
            if (dbInstance == null) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("route sql {} to {}", sql, dbInstance);
            }
            dbInstance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (Exception e) {
            rwSplitService.executeException(e, sql);
        }
    }

    public void setRwGroup(PhysicalDbGroup rwGroup) {
        this.rwGroup = rwGroup;
    }

    public void bind(BackendConnection bindConn) {
        final BackendConnection tmp = conn;
        if (tmp != null && tmp != bindConn) {
            LOGGER.warn("last conn is remaining, the session is {}, the backend conn is {}", rwSplitService.getConnection(), tmp);
            tmp.release();
        }
        LOGGER.debug("bind conn is {}", bindConn);
        this.conn = bindConn;
    }

    public void unbindIfSafe() {
        final BackendConnection tmp = conn;
        if (tmp != null && rwSplitService.isKeepBackendConn()) {
            this.conn = null;
            if (rwSplitService.isFlowControlled()) {
                releaseConnectionFromFlowControlled(tmp);
            }
            LOGGER.debug("safe unbind conn is {}", tmp);
            tmp.release();
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public void close(String reason) {
        if (null != rwGroup) {
            rwGroup.unBindRwSplitSession(this);
        }
        final BackendConnection tmp = this.conn;
        this.conn = null;
        if (tmp != null) {
            tmp.close(reason);
        }
    }

    public BackendConnection getConn() {
        return conn;
    }

    public boolean closed() {
        return rwSplitService.getConnection().isClosed();
    }

    public RWSplitService getService() {
        return rwSplitService;
    }
}
