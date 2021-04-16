/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionListener;
import com.actiontech.dble.backend.pool.util.TimerHolder;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import io.netty.util.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * heartbeat check for mysql connections
 *
 * @author wuzhih
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {

    private final Object heartbeatLock;
    private volatile Timeout heartbeatTimeout;
    private final BackendConnection conn;
    private final MySQLConnectionListener listener;
    private boolean returned = false;
    private boolean finished = false;

    public ConnectionHeartBeatHandler(BackendConnection conn, boolean isBlock, MySQLConnectionListener listener) {
        conn.setResponseHandler(this);
        this.conn = conn;
        this.listener = listener;
        if (isBlock) {
            this.heartbeatLock = new Object();
        } else {
            this.heartbeatLock = null;
        }
    }

    public boolean ping(long timeout) {
        if (heartbeatLock != null) {
            final long deadline = System.currentTimeMillis() + timeout;
            synchronized (heartbeatLock) {
                conn.ping();
                try {
                    while (!returned) {
                        timeout = deadline - System.currentTimeMillis();
                        if (timeout <= 0L) {
                            returned = true;
                        } else {
                            heartbeatLock.wait(timeout);
                        }
                    }
                } catch (InterruptedException e) {
                    returned = true;
                }
            }
        } else {
            heartbeatTimeout = TimerHolder.getTimer().newTimeout(timeout1 -> conn.closeWithoutRsp("conn heart timeout"), timeout, TimeUnit.MILLISECONDS);
            conn.ping();
        }

        return finished;
    }

    /**
     * if the query returns ok than just release the connection
     * and go on check the next one
     *
     * @param ok
     * @param con
     */
    @Override
    public void okResponse(byte[] ok, BackendConnection con) {
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                if (!returned) {
                    returned = true;
                    finished = true;
                    heartbeatLock.notifyAll();
                }
            }
            return;
        }

        heartbeatTimeout.cancel();
        listener.onHeartbeatSuccess(con);
    }

    @Override
    public void connectionClose(BackendConnection con, String reason) {
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                if (!returned) {
                    returned = true;
                    finished = false;
                    heartbeatLock.notifyAll();
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection con) {
        // not called
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection con) {
        // not called
        return false;
    }

    /**
     * @param eof
     * @param isLeft
     * @param con
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection con) {
        // not called
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        // not called
    }

    @Override
    public void connectionAcquired(BackendConnection con) {
        // not called
    }

    /**
     * if heart beat returns error than clase the connection and
     * start the next one
     *
     * @param data
     * @param con
     */
    @Override
    public void errorResponse(byte[] data, BackendConnection con) {
    }
}
