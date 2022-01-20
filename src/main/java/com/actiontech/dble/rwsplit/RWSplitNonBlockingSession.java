package com.actiontech.dble.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.RouteService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashSet;
import java.util.Set;


public class RWSplitNonBlockingSession extends Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;
    private Set<String> nameSet = new HashSet<>();
    private int reSelectNum;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    @Override
    public FrontendConnection getSource() {
        return (FrontendConnection) rwSplitService.getConnection();
    }

    public void execute(Boolean master, Callback callback) {
        execute(master, null, callback);
    }

    public void execute(Boolean master, Callback callback, boolean write) {
        execute(master, null, callback, write);
    }

    public void execute(Boolean master, Callback callback, String sql) {
        try {
            SQLStatement statement = parseSQL(sql);
            if (statement instanceof MySqlPrepareStatement) {
                String simpleName = ((MySqlPrepareStatement) statement).getName().getSimpleName();
                nameSet.add(simpleName);
                rwSplitService.setInPrepare(true);
            }
            if (statement instanceof MysqlDeallocatePrepareStatement) {
                String simpleName = ((MysqlDeallocatePrepareStatement) statement).getStatementName().getSimpleName();
                nameSet.remove(simpleName);
                if (nameSet.isEmpty()) {
                    rwSplitService.setInPrepare(false);
                }
            }
        } catch (SQLSyntaxErrorException throwables) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");
        }
        execute(master, null, callback);
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback) {
        try {
            RWSplitHandler handler = getRwSplitHandler(originPacket, callback);
            if (handler == null) return;
            getConnection(handler, master, null);
        } catch (SQLSyntaxErrorException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }

    public void execute(Boolean master, byte[] originPacket, Callback callback, boolean write) {
        try {
            RWSplitHandler handler = getRwSplitHandler(originPacket, callback);
            if (handler == null) return;
            getConnection(handler, master, isWrite(write));
        } catch (SQLSyntaxErrorException se) {
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
    private RWSplitHandler getRwSplitHandler(byte[] originPacket, Callback callback) throws SQLSyntaxErrorException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback, false);
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            checkDest(!conn.getInstance().isReadInstance());
            handler.execute(conn);
            return null;
        }
        return handler;
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

    private SQLStatement parseSQL(String stmt) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        try {
            return parser.parseStatement();
        } catch (Exception t) {
            throw new SQLSyntaxErrorException(t);
        }
    }

    public PhysicalDbGroup reSelectRWDbGroup(PhysicalDbGroup dbGroup) {
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
            dbInstance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (Exception e) {
            rwSplitService.executeException(e, sql);
            return;
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
                !rwSplitService.isInPrepare() && this.conn != null && !rwSplitService.isUsingTmpTable()) {
            this.conn.release();
            this.conn = null;
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public void close(String reason) {
        if (null != rwGroup) {
            rwGroup.unBindRwSplitSession(this);
        }
        if (conn != null) {
            conn.close(reason);
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
