/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.PingPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * heartbeat check for mysql connections
 *
 * @author wuzhih
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHeartBeatHandler.class);

    protected final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();


    public void doHeartBeat(BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("do heartbeat for con " + conn);
        }
        lock.lock();
        try {
            conn.setResponseHandler(this);
            MySQLConnection mcon = (MySQLConnection) conn;
            mcon.write(mcon.writeToBuffer(PingPacket.PING, mcon.allocate()));
            long validateTime = 2;
            if (!condition.await(validateTime, TimeUnit.SECONDS)) {
                //if the thread be waked up by timer than close the connection
                conn.close("heartbeat timeout ");
            }
        } catch (Exception e) {
            executeException(conn, e);
        } finally {
            lock.unlock();
        }
    }


    /**
     * if the query returns ok than just release the connection
     * and go on check the next one
     *
     * @param ok
     * @param conn
     */
    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        lock.lock();
        try {
            condition.signal();
            conn.release();
        } finally {
            lock.unlock();
        }
    }

    /**
     * if heart beat returns error than clase the connection and
     * start the next one
     *
     * @param data
     * @param conn
     */
    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        lock.lock();
        try {
            condition.signal();
            conn.close("heatbeat return error");
        } finally {
            lock.unlock();
        }
    }

    /**
     * if the heartbeat throws the Exception than close the connection
     *
     * @param c
     * @param e
     */
    private void executeException(BackendConnection c, Throwable e) {
        lock.lock();
        try {
            condition.signal();
            c.close("heatbeat exception:" + e);
            LOGGER.warn("executeException   ", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * if when the query going on the conneciton be closed
     * than just do nothing and go on for next one
     *
     * @param conn
     * @param reason
     */
    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        lock.lock();
        try {
            condition.signal();
            LOGGER.warn("connection closed " + conn + " reason:" + reason);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param eof
     * @param isLeft
     * @param conn
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        // not called
    }


    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        // not called
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        // not called
        return false;
    }

    @Override
    public void writeQueueAvailable() {
        // not called
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
        // not called
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
        // not called
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        // not called
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        // not called
    }
}
