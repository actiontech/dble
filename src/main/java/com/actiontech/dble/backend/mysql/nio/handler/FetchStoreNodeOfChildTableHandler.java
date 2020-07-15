/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the shardingNode to store child table's records
 *
 * @author wuzhih, huqing.yan
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchStoreNodeOfChildTableHandler.class);
    private final String sql;
    private AtomicBoolean hadResult = new AtomicBoolean(false);
    private volatile String shardingNode;
    private Map<String, RouteResultsetNode> receiveMap = new ConcurrentHashMap<>();
    private Map<String, String> nodesErrorReason = new ConcurrentHashMap<>();
    protected final ReentrantLock lock = new ReentrantLock();
    private Condition result = lock.newCondition();
    private final NonBlockingSession session;

    public FetchStoreNodeOfChildTableHandler(String sql, NonBlockingSession session) {
        this.sql = sql;
        this.session = session;
    }

    public String execute(String schema, List<String> shardingNodes) throws ConnectionException {
        String key = schema + ":" + sql;
        CachePool cache = CacheService.getCachePoolByName("ER_SQL2PARENTID");
        if (cache != null) {
            String cacheResult = (String) cache.get(key);
            if (cacheResult != null) {
                return cacheResult;
            }
        }

        int totalCount = shardingNodes.size();

        LOGGER.debug("find child node with sql:" + sql);
        for (String dn : shardingNodes) {
            if (!LOGGER.isDebugEnabled()) {
                //no early return when debug
                if (shardingNode != null) {
                    LOGGER.debug(" found return ");
                    return shardingNode;
                }
            }
            ShardingNode mysqlDN = DbleServer.getInstance().getConfig().getShardingNodes().get(dn);
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("execute in shardingNode " + dn);
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
                    conn.setSession(session);
                    ((MySQLConnection) conn).setComplexQuery(true);
                    conn.execute(node, session.getSource(), false);
                } else {
                    mysqlDN.getConnection(mysqlDN.getDatabase(), session.getSource().isTxStart(), session.getSource().isAutocommit(), node, this, node);
                }
            } catch (Exception e) {
                LOGGER.info("get connection err " + e);
            }
        }
        lock.lock();
        try {
            while (receiveMap.size() < totalCount) {
                try {
                    result.await(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }

        if (nodesErrorReason.size() != 0) {
            StringBuilder fatalErrorMsg = new StringBuilder("Find (root) parent sharding node occur error: {");
            for (Map.Entry<String, String> nodeErrorReason : nodesErrorReason.entrySet()) {
                String node = nodeErrorReason.getKey();
                String reason = nodeErrorReason.getValue();
                fatalErrorMsg.append("[").append(node).append(":").append(reason).append("],");
            }
            fatalErrorMsg.append("}");
            throw new ConnectionException(ErrorCode.ER_UNKNOWN_ERROR, fatalErrorMsg.toString());
        }

        if (!LOGGER.isDebugEnabled()) {
            //no cached when debug
            if (shardingNode != null && cache != null) {
                cache.putIfAbsent(key, shardingNode);
            }
        }
        return shardingNode;

    }

    /**
     * Only when the connection in the target,the connection can not be release
     * even if the session is close ,the session close would get the connection
     * from target and close it
     * So wo just think about one question?Is this connection need release
     *
     * @param con BackendConnection
     */
    private void releaseConnIfSafe(BackendConnection con) {
        RouteResultsetNode node = (RouteResultsetNode) con.getAttachment();
        if (session.getTarget(node) != con) {
            con.release();
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        conn.setSession(session);
        try {
            conn.query(sql);
        } catch (Exception e) {
            executeException(conn, e);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        nodesErrorReason.put(((RouteResultsetNode) attachment).getName(), "connectionError");
        countResult((RouteResultsetNode) attachment);
        LOGGER.info("connectionError " + e);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg = new String(err.getMessage());
        LOGGER.info("errorResponse " + err.getErrNo() + " " + msg);
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            nodesErrorReason.put(((RouteResultsetNode) conn.getAttachment()).getName(), msg);
            releaseConnIfSafe(conn);
        } else {
            nodesErrorReason.put(((RouteResultsetNode) conn.getAttachment()).getName(), "sync context error:" + msg);
            RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
            if (session.getTarget(node) == conn) {
                session.getTargetMap().remove(node);
            }
            conn.closeWithoutRsp("unfinished sync");
        }
        countResult((RouteResultsetNode) conn.getAttachment());
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("okResponse " + conn);
        }
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            countResult((RouteResultsetNode) conn.getAttachment());
            releaseConnIfSafe(conn);
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
                shardingNode = ((RouteResultsetNode) conn.getAttachment()).getName();
                result.signal();
            } finally {
                lock.unlock();
            }
        } else {
            LOGGER.info("find multi shardingNodes for child table store, sql is:  " + sql);
        }
        return false;
    }


    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("rowEofResponse" + conn);
        }
        countResult((RouteResultsetNode) conn.getAttachment());
        releaseConnIfSafe(conn);
    }

    private void executeException(BackendConnection c, Throwable e) {
        nodesErrorReason.put(((RouteResultsetNode) c.getAttachment()).getName(), e.getMessage());
        countResult((RouteResultsetNode) c.getAttachment());
        LOGGER.info("executeException   " + e);
        releaseConnIfSafe(c);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.info("connection closed " + conn + " reason:" + reason);
        nodesErrorReason.put(((RouteResultsetNode) conn.getAttachment()).getName(), "connection closed ,mysql id:" + ((MySQLConnection) conn).getThreadId());
        countResult((RouteResultsetNode) conn.getAttachment());
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }


    private void countResult(RouteResultsetNode routeNode) {
        receiveMap.put(routeNode.getName(), routeNode);
    }
}
