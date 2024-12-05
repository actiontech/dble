/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.backend.pool.PooledConnectionListener;
import com.oceanbase.obsharding_d.backend.pool.util.TimerHolder;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.PooledConnection;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.PingPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import io.netty.util.Timeout;
import org.jetbrains.annotations.NotNull;

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
    private boolean returned = false;
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
        if (heartbeatLock != null) {
            final long deadline = System.currentTimeMillis() + timeout;
            synchronized (heartbeatLock) {
                conn.getService().write(PingPacket.PING, WriteFlags.QUERY_END);
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
            heartbeatTimeout = TimerHolder.getTimer().newTimeout(timeout1 -> conn.businessClose("conn heart timeout"), timeout, TimeUnit.MILLISECONDS);
            conn.getService().write(PingPacket.PING, WriteFlags.QUERY_END);
        }

        return finished;
    }

    /**
     * if the query returns ok than just release the connection
     * and go on check the next one
     *
     * @param ok
     * @param service
     */
    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
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
        listener.onHeartbeatSuccess((PooledConnection) service.getConnection());
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
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
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {

    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        // not called
    }

    @Override
    public void connectionAcquired(com.oceanbase.obsharding_d.net.connection.BackendConnection connection) {

    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {

    }
}
