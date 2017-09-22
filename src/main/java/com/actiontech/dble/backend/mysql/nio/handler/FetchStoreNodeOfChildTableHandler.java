/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the data node to store child table's records
 *
 * @author wuzhih, huqing.yan
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchStoreNodeOfChildTableHandler.class);
    private final String sql;
    private AtomicBoolean hadResult = new AtomicBoolean(false);
    private volatile String dataNode;
    private AtomicInteger finished = new AtomicInteger(0);
    protected final ReentrantLock lock = new ReentrantLock();
    private Condition result = lock.newCondition();
    private final NonBlockingSession session;

    public FetchStoreNodeOfChildTableHandler(String sql, NonBlockingSession session) {
        this.sql = sql;
        this.session = session;
    }

    public String execute(String schema, ArrayList<String> dataNodes) {
        String key = schema + ":" + sql;
        CachePool cache = DbleServer.getInstance().getCacheService().getCachePool("ER_SQL2PARENTID");
        if (cache != null) {
            String cacheResult = (String) cache.get(key);
            if (cacheResult != null) {
                return cacheResult;
            }
        }

        int totalCount = dataNodes.size();
        ServerConfig conf = DbleServer.getInstance().getConfig();

        LOGGER.debug("find child node with sql:" + sql);
        for (String dn : dataNodes) {
            if (!LOGGER.isDebugEnabled()) {
                //no early return when debug
                if (dataNode != null) {
                    LOGGER.debug(" found return ");
                    return dataNode;
                }
            }
            PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("execute in data_node " + dn);
                }
                RouteResultsetNode node = new RouteResultsetNode(dn, ServerParse.SELECT, sql);
                node.setRunOnSlave(false); // get child node from master
                BackendConnection conn = session.getTarget(node);
                if (session.tryExistsCon(conn, node)) {
                    if (session.closed()) {
                        session.clearResources(true);
                        return null;
                    }
                    conn.setResponseHandler(this);
                    conn.execute(node, session.getSource(), isAutoCommit());

                } else {
                    mysqlDN.getConnection(mysqlDN.getDatabase(), true, node, this, node);
                }
            } catch (Exception e) {
                LOGGER.warn("get connection err " + e);
            }
        }
        lock.lock();
        try {
            while (dataNode == null) {
                try {
                    result.await(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }
                if (dataNode != null || finished.get() >= totalCount) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        if (!LOGGER.isDebugEnabled()) {
            //no cached when debug
            if (dataNode != null && cache != null) {
                cache.putIfAbsent(key, dataNode);
            }
        }
        return dataNode;

    }

    private boolean isAutoCommit() {
        return session.getSource().isAutocommit() && !session.getSource().isTxstart();
    }

    private boolean canReleaseConn() {
        if (session.getSource().isClosed()) {
            return false;
        }
        return isAutoCommit();
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        try {
            conn.query(sql);
        } catch (Exception e) {
            executeException(conn, e);
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        finished.incrementAndGet();
        LOGGER.warn("connectionError " + e);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        finished.incrementAndGet();
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        LOGGER.warn("errorResponse " + err.getErrno() + " " + new String(err.getMessage()));
        if (canReleaseConn()) {
            conn.release();
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("okResponse " + conn);
        }
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            finished.incrementAndGet();
            if (canReleaseConn()) {
                conn.release();
            }
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received rowResponse response from  " + conn);
        }
        if (hadResult.compareAndSet(false, true)) {
            lock.lock();
            try {
                dataNode = ((RouteResultsetNode) conn.getAttachment()).getName();
                result.signal();
            } finally {
                lock.unlock();
            }
        } else {
            LOGGER.warn("find multi data nodes for child table store, sql is:  " + sql);
        }
        return false;
    }


    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("rowEofResponse" + conn);
        }
        finished.incrementAndGet();
        if (canReleaseConn()) {
            conn.release();
        }
    }

    private void executeException(BackendConnection c, Throwable e) {
        finished.incrementAndGet();
        LOGGER.warn("executeException   " + e);
        if (canReleaseConn()) {
            c.release();
        }
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("connection closed " + conn + " reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }
}
