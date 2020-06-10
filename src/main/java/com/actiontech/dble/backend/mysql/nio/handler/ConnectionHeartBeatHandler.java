/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.List;

/**
 * heartbeat check for mysql connections
 *
 * @author wuzhih
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {

    /**
     * if the query returns ok than just release the connection
     * and go on check the next one
     *
     * @param ok
     * @param conn
     */
    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        conn.pong();
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
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        // not called
        return false;
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
