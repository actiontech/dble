/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NewConnectionRespHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewConnectionRespHandler.class);
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
