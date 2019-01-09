/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NewConnectionRespHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewConnectionRespHandler.class);
    private BackendConnection backConn;
    private ReentrantLock lock = new ReentrantLock();
    private Condition initiated = lock.newCondition();
    private String errMsg;
    public BackendConnection getBackConn() throws IOException {
        lock.lock();
        try {
            if (backConn == null) {
                initiated.await();
            }
            if (backConn == null) {
                throw new IOException(errMsg);
            }
            return backConn;
        } catch (InterruptedException e) {
            LOGGER.info("getBackConn " + e);
            throw new IOException(e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info(conn + " connectionError " + e);
        errMsg = "Backend connect Error, Connection{DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        lock.lock();
        try {
            initiated.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        lock.lock();
        try {
            backConn = conn;
            initiated.signal();
        } finally {
            lock.unlock();
        }
        LOGGER.info("connectionAcquired " + conn);


    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        LOGGER.info("caught error resp: " + conn + " " + new String(err));
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

}
