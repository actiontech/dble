/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private volatile int txIsolation;
    private volatile boolean autocommit;
    private volatile boolean txStarted;
    private volatile boolean txChainBegin;
    private volatile boolean txInterrupted;
    private volatile String txInterrputMsg = "";
    private long lastInsertId;
    private NonBlockingSession session;
    private volatile boolean isLocked = false;
    private AtomicLong txID;

    public long getAndIncrementXid() {
        return txID.getAndIncrement();
    }

    public long getXid() {
        return txID.get();
    }

    public ServerConnection(NetworkChannel channel)
            throws IOException {
        super(channel);
        this.txInterrupted = false;
        this.autocommit = true;
        this.txID = new AtomicLong(1);
    }

    public ServerConnection() {
        /* just for unit test */
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
                    lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public boolean isTxstart() {
        return txStarted;
    }

    public void setTxstart(boolean txStart) {
        if (!txStart && txChainBegin) {
            txChainBegin = false;
        } else {
            this.txStarted = txStart;
        }
    }

    public void setTxInterrupt(String msg) {
        if ((!autocommit || txStarted) && !txInterrupted) {
            txInterrupted = true;
            this.txInterrputMsg = "Transaction error, need to rollback.Reason:[" + msg + "]";
        }
    }

    public NonBlockingSession getSession2() {
        return session;
    }

    public void setSession2(NonBlockingSession session2) {
        this.session = session2;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        this.isLocked = locked;
    }

    @Override
    public void ping() {
        Ping.response(this);
    }

    @Override
    public void heartbeat(byte[] data) {
        Heartbeat.response(this, data);
    }

    public void execute(String sql, int type) {
        if (this.isClosed()) {
            LOGGER.warn("ignore execute ,server connection is closed " + this);
            return;
        }
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterrputMsg);
            return;
        }

        String db = this.schema;

        SchemaConfig schemaConfig = null;
        if (db != null) {
            schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(db);
            if (schemaConfig == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }
        routeEndExecuteSQL(sql, type, schemaConfig);

    }

    public RouteResultset routeSQL(String sql, int type) {
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No Database selected");
            return null;
        }
        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
            return null;
        }

        RouteResultset rrs;
        try {
            rrs = DbleServer.getInstance().getRouterService().route(schema, type, sql, this);
        } catch (Exception e) {
            executeException(e, sql);
            return null;
        }
        return rrs;
    }


    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig user = conf.getUsers().get(this.getUser());
        if (user == null || !user.getSchemas().contains(schemaInfo.getSchema())) {
            writeErrMessage("42000", "Access denied for user '" + this.getUser() + "' to database '" + schemaInfo.getSchema() + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        RouteResultset rrs = new RouteResultset(stmt, sqlType);
        try {
            if (RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable())) {
                RouterUtil.routeToSingleNode(rrs, schemaInfo.getSchemaConfig().getDataNode());
            } else {
                TableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
                if (tc == null) {
                    String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                    writeErrMessage("42S02", msg, ErrorCode.ER_NO_SUCH_TABLE);
                    return;
                }
                RouterUtil.routeToRandomNode(rrs, schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            }
            session.execute(rrs, sqlType);
        } catch (Exception e) {
            executeException(e, stmt);
        }
    }

    private void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
        RouteResultset rrs;
        try {
            rrs = DbleServer.getInstance().getRouterService().route(schema, type, sql, this);
            if (rrs == null) {
                return;
            }
            if (rrs.getSqlType() == ServerParse.DDL) {
                addTableMetaLock(rrs);
            }
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }
        session.execute(rrs, type);
    }

    private void addTableMetaLock(RouteResultset rrs) throws SQLNonTransientException {
        String schema = rrs.getSchema();
        String table = rrs.getTable();
        try {
            DbleServer.getInstance().getTmManager().addMetaLock(schema, table);
            if (DbleServer.getInstance().isUseZK()) {
                String nodeName = StringUtil.getFullName(schema, table);
                String ddlPath = KVPathUtil.getDDLPath();
                String nodePth = ZKPaths.makePath(ddlPath, nodeName);
                CuratorFramework zkConn = ZKUtils.getConnection();
                int times = 0;
                while (zkConn.checkExists().forPath(KVPathUtil.getSyncMetaLockPath()) != null || zkConn.checkExists().forPath(nodePth) != null) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    if (times % 60 == 0) {
                        LOGGER.warn("waiting for syncMeta.lock or metaLock about " + nodeName + " in " + ddlPath);
                        times = 0;
                    }
                    times++;
                }
                DbleServer.getInstance().getTmManager().notifyClusterDDL(schema, table, rrs.getStatement(), DDLInfo.DDLStatus.INIT, true);
            }
        } catch (Exception e) {
            DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
            throw new SQLNonTransientException(e.toString() + ",sql:" + rrs.getStatement());
        }
    }

    private void executeException(Exception e, String sql) {
        if (e instanceof SQLException) {
            SQLException sqle = (SQLException) e;
            String msg = sqle.getMessage();
            StringBuilder s = new StringBuilder();
            LOGGER.info(s.append(this).append(sql).toString() + " err:" + msg);
            int vendorCode = sqle.getErrorCode() == 0 ? ErrorCode.ER_PARSE_ERROR : sqle.getErrorCode();
            String sqlState = StringUtil.isEmpty(sqle.getSQLState()) ? "HY000" : sqle.getSQLState();
            String errorMsg = msg == null ? sqle.getClass().getSimpleName() : msg;
            writeErrMessage(sqlState, errorMsg, vendorCode);
        } else {
            StringBuilder s = new StringBuilder();
            LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        }
    }

    /**
     * begin without commit means commit and begin
     */
    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterrputMsg);
        } else {
            TxnLogHelper.putTxnLog(this, "commit[because of " + stmt + "]");
            this.txChainBegin = true;
            session.commit();
            TxnLogHelper.putTxnLog(this, stmt);
        }
    }

    public void commit(String logReason) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterrputMsg);
        } else {
            TxnLogHelper.putTxnLog(this, logReason);
            session.commit();
        }
    }

    public void rollback() {
        if (txInterrupted) {
            txInterrupted = false;
        }

        session.rollback();
    }

    /**
     * @param sql
     */
    public void lockTable(String sql) {
        // lock table is disable in transaction
        if (!autocommit) {
            writeErrMessage(ErrorCode.ER_YES, "can't lock table in transaction!");
            return;
        }
        // if lock table has been executed and unlock has not been executed, can't execute lock table again
        if (isLocked) {
            writeErrMessage(ErrorCode.ER_YES, "can't lock multi-table");
            return;
        }
        RouteResultset rrs = routeSQL(sql, ServerParse.LOCK);
        if (rrs != null) {
            session.lockTable(rrs);
        }
    }

    /**
     * @param sql
     */
    public void unLockTable(String sql) {
        sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
        String[] words = SplitUtil.split(sql, ' ', true);
        if (words.length == 2 && ("table".equalsIgnoreCase(words[1]) || "tables".equalsIgnoreCase(words[1]))) {
            isLocked = false;
            session.unLockTable(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }

    }

    @Override
    public void close(String reason) {

        //XA transaction in this phase,close it
        if (session.getSource().isTxstart() && session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_CANCELING) &&
                session.getXaState() != null && session.getXaState() != TxState.TX_INITIALIZE_STATE) {
            super.close(reason);
            session.initiativeTerminate();
        } else if (session.getSource().isTxstart() &&
                session.getXaState() != null && session.getXaState() != TxState.TX_INITIALIZE_STATE) {
            //XA transaction in this phase(commit/rollback) close the front end and wait for the backend finished
            super.close(reason);
        } else {
            //not a xa transaction ,close it
            super.close(reason);
            session.terminate();
        }

        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
    }




    @Override
    public void killAndClose(String reason) {
        if (session.getSource().isTxstart() && !session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_CANCELING) &&
                  session.getXaState() != null && session.getXaState() != TxState.TX_INITIALIZE_STATE) {
            //XA transaction in this phase(commit/rollback) close the front end and wait for the backend finished
            super.close(reason);
        } else {
            //not a xa transaction ,close it
            super.close(reason);
            session.kill();
        }
    }

    @Override
    public String toString() {
        return "ServerConnection [id=" + id + ", schema=" + schema + ", host=" + host +
                ", user=" + user + ",txIsolation=" + txIsolation + ", autocommit=" + autocommit +
                ", schema=" + schema + "]";
    }

}
