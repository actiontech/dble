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
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.List;

/**
 * @author mycat
 * @author mycat
 */
public interface ResponseHandler {

    /**
     * can't get an valid connection
     *
     * @param e
     * @param conn
     */
    void connectionError(Throwable e, BackendConnection conn);

    /**
     * execute after acquired an valid connection
     */
    void connectionAcquired(BackendConnection conn);

    /**
     * execute after get an error response
     */
    void errorResponse(byte[] err, BackendConnection conn);

    /**
     * execute after get an OK response
     */
    void okResponse(byte[] ok, BackendConnection conn);

    /**
     * execute after get an fieldEof response
     */

    void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                          boolean isLeft, BackendConnection conn);

    /**
     * execute after get an row response
     */
    boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn);

    /**
     * execute after get an rowEof response
     */
    void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn);

    /**
     * execute after get an relayPacket response
     */
    void relayPacketResponse(byte[] relayPacket, BackendConnection conn);

    /**
     * execute after get an endPacket response
     */
    void endPacketResponse(byte[] endPacket, BackendConnection conn);

    void writeQueueAvailable();

    /**
     * on connection close event
     */
    void connectionClose(BackendConnection conn, String reason);

}
