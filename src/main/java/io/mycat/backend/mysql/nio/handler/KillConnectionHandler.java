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
import io.mycat.net.mysql.*;
import io.mycat.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author mycat
 */
public class KillConnectionHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillConnectionHandler.class);

    private final MySQLConnection killee;
    private final NonBlockingSession session;

    public KillConnectionHandler(BackendConnection killee,
                                 NonBlockingSession session) {
        this.killee = (MySQLConnection) killee;
        this.session = session;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        CommandPacket packet = new CommandPacket();
        packet.setPacketId(0);
        packet.setCommand(MySQLPacket.COM_QUERY);
        packet.setArg(("KILL " + killee.getThreadId()).getBytes());
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        packet.write(mysqlCon);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        killee.close("exception:" + e.toString());
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("kill connection success connection id:" +
                    killee.getThreadId());
        }
        conn.release();
        killee.close("killed");

    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
        conn.quit();
        killee.close("killed");
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg = null;
        try {
            msg = new String(err.getMessage(), conn.getCharset());
        } catch (UnsupportedEncodingException e) {
            msg = new String(err.getMessage());
        }
        LOGGER.warn("kill backend connection " + killee + " failed: " + msg + " con:" + conn);
        conn.release();
        killee.close("exception:" + msg);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        return false;
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
