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
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NewConnectionRespHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(NewConnectionRespHandler.class);
    private BackendConnection backConn;
    private ReentrantLock lock = new ReentrantLock();
    private Condition inited = lock.newCondition();

    public BackendConnection getBackConn() {
        lock.lock();
        try {
            while (backConn == null) {
                inited.await();
            }
            return backConn;
        } catch (InterruptedException e) {
            LOGGER.warn("getBackConn " + e);
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn(conn + " connectionError " + e);

    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        lock.lock();
        try {
            backConn = conn;
            inited.signal();
        } finally {
            lock.unlock();
        }
        LOGGER.info("connectionAcquired " + conn);


    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        LOGGER.warn("caught error resp: " + conn + " " + new String(err));
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        LOGGER.info("okResponse: " + conn);

    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.info("fieldEofResponse: " + conn);

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.info("rowResponse: " + conn);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.info("rowEofResponse: " + conn);

    }

    @Override
    public void writeQueueAvailable() {


    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {


    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }

}
