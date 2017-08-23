/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.loader.zkprocess.zookeeper.process.DDLInfo;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.net.FrontendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.Heartbeat;
import io.mycat.server.response.Ping;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.*;
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
    /**
     * 标志是否执行了lock tables语句，并处于lock状态
     */
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

    /**
     * 设置是否需要中断当前事务
     */
    public void setTxInterrupt(String txInterrputMsg) {
        if ((!autocommit || txStarted) && !txInterrupted) {
            txInterrupted = true;
            this.txInterrputMsg = "Transaction error, need to rollback.Reason:[" + txInterrputMsg + "]";
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

    public void setLocked(boolean isLocked) {
        this.isLocked = isLocked;
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
        //连接状态检查
        if (this.isClosed()) {
            LOGGER.warn("ignore execute ,server connection is closed " + this);
            return;
        }
        // 事务状态检查
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterrputMsg);
            return;
        }

        // 检查当前使用的DB
        String db = this.schema;

        SchemaConfig schemaConfig = null;
        if (db != null) {
            schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(db);
            if (schemaConfig == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }
        routeEndExecuteSQL(sql, type, schemaConfig);

    }

    public RouteResultset routeSQL(String sql, int type) {
        // 检查当前使用的DB
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No Database selected");
            return null;
        }
        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
            return null;
        }

        // 路由计算
        RouteResultset rrs;
        try {
            rrs = MycatServer.getInstance().getRouterService().route(schema, type, sql, this.charset, this);
        } catch (Exception e) {
            executeException(e, sql);
            return null;
        }
        return rrs;
    }


    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        UserConfig user = conf.getUsers().get(this.getUser());
        if (user == null || !user.getSchemas().contains(schemaInfo.schema)) {
            writeErrMessage("42000", "Access denied for user '" + this.getUser() + "' to database '" + schemaInfo.schema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        RouteResultset rrs = new RouteResultset(stmt, sqlType);
        try {
            if (RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table)) {
                RouterUtil.routeToSingleNode(rrs, schemaInfo.schemaConfig.getDataNode());
            } else {
                TableConfig tc = schemaInfo.schemaConfig.getTables().get(schemaInfo.table);
                if (tc == null) {
                    String msg = "Table '" + schemaInfo.schema + "." + schemaInfo.table + "' doesn't exist";
                    writeErrMessage("42S02", msg, ErrorCode.ER_NO_SUCH_TABLE);
                    return;
                }
                RouterUtil.routeToRandomNode(rrs, schemaInfo.schemaConfig, schemaInfo.table);
            }
            session.execute(rrs, sqlType);
        } catch (Exception e) {
            executeException(e, stmt);
        }
    }

    private void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
        // 路由计算
        RouteResultset rrs;
        try {
            rrs = MycatServer.getInstance().getRouterService().route(schema, type, sql, this.charset, this);
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
        // session执行
        session.execute(rrs, type);
    }

    private void addTableMetaLock(RouteResultset rrs) throws SQLNonTransientException {
        String schema = rrs.getSchema();
        String table = rrs.getTable();
        try {
            MycatServer.getInstance().getTmManager().addMetaLock(schema, table);
            if (MycatServer.getInstance().isUseZK()) {
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
                MycatServer.getInstance().getTmManager().notifyClusterDDL(schema, table, rrs.getStatement(), DDLInfo.DDLStatus.INIT, true);
            }
        } catch (Exception e) {
            MycatServer.getInstance().getTmManager().removeMetaLock(schema, table);
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
     * 事务沒有commit 直接begin
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

    /**
     * 提交事务
     */
    public void commit(String logReason) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterrputMsg);
        } else {
            TxnLogHelper.putTxnLog(this, logReason);
            session.commit();
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        // 状态检查
        if (txInterrupted) {
            txInterrupted = false;
        }

        // 执行回滚
        session.rollback();
    }

    /**
     * 执行lock tables语句方法
     *
     * @param sql
     */
    public void lockTable(String sql) {
        // 事务中不允许执行lock table语句
        if (!autocommit) {
            writeErrMessage(ErrorCode.ER_YES, "can't lock table in transaction!");
            return;
        }
        // 已经执行了lock table且未执行unlock table之前的连接不能再次执行lock table命令
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
     * 执行unlock tables语句方法
     *
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

        //处于第一阶段的XA事务，可以通过关闭的方式回滚
        if (session.getSource().isTxstart() && session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_CANCELING) &&
                session.getXaState() != null && session.getXaState() != TxState.TX_INITIALIZE_STATE) {
            super.close(reason);
            session.initiativeTerminate();
        } else if (session.getSource().isTxstart() &&
                session.getXaState() != null && session.getXaState() != TxState.TX_INITIALIZE_STATE) {
            //已经提交了commit/rollback的XA事务，关闭前端，待后端自动完成
            super.close(reason);
        } else {
            //并没有开启XA事务，直接关闭前后端连接
            super.close(reason);
            session.terminate();
        }

        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
    }

    @Override
    public String toString() {
        return "ServerConnection [id=" + id + ", schema=" + schema + ", host=" + host +
                ", user=" + user + ",txIsolation=" + txIsolation + ", autocommit=" + autocommit +
                ", schema=" + schema + "]";
    }

}
