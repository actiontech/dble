/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.util.TimerHolder;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
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
    private final PooledConnectionListener listener;
    private boolean finished = false;

    public ConnectionHeartBeatHandler(BackendConnection conn, boolean isBlock, PooledConnectionListener listener) {
        conn.getBackendService().setResponseHandler(this);
        this.conn = conn;
        this.listener = listener;
        if (isBlock) {
            this.heartbeatLock = new Object();
        } else {
            this.heartbeatLock = null;
        }
    }

    public boolean ping(long timeout) {
        conn.getBackendService().ping();
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
                    conn.businessClose("conn heart timeout");
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
     * @param service
     */
    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                finished = true;
                heartbeatLock.notifyAll();
            }
            return;
        }

        heartbeatTimeout.cancel();
        listener.onHeartbeatSuccess((PooledConnection) service.getConnection());
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {

    }

    @Override
    public void connectionClose(AbstractService service, String reason) {

    }


    @Override
    public void connectionError(Throwable e, Object attachment) {
        // not called
    }

    @Override
    public void connectionAcquired(com.actiontech.dble.net.connection.BackendConnection connection) {

    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {

    }
}
