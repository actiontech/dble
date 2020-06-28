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
import io.netty.util.TimerTask;

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
        conn.ping();
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                try {
                    heartbeatLock.wait(timeout);
                } catch (InterruptedException e) {
                    finished = false;
                }
            }
            return finished;
        } else {
            heartbeatTimeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    conn.closeWithoutRsp("conn heart timeout");
                }
            }, timeout, TimeUnit.MILLISECONDS);
            return true;
        }
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
                finished = true;
                heartbeatLock.notifyAll();
            }
            return;
        }

        heartbeatTimeout.cancel();
        listener.onHeartbeatSuccess(con);
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

    /**
     * if when the query going on the conneciton be closed
     * than just do nothing and go on for next one
     *
     * @param con
     * @param reason
     */
    @Override
    public void connectionClose(BackendConnection con, String reason) {

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
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection con) {
        // not called
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection con) {
        // not called
        return false;
    }

    @Override
    public void connectionAcquired(BackendConnection con) {
        // not called
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        // not called
    }
}
