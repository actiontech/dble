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
package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.PingPacket;
import io.mycat.net.mysql.RowDataPacket;
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
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ConnectionHeartBeatHandler.class);

    protected final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();

    private final long validateTime = 2;


    public void doHeartBeat(BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("do heartbeat for con " + conn);
        }
        lock.lock();
        try {
            conn.setResponseHandler(this);
            MySQLConnection mcon = (MySQLConnection) conn;
            mcon.write(mcon.writeToBuffer(PingPacket.PING, mcon.allocate()));
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
