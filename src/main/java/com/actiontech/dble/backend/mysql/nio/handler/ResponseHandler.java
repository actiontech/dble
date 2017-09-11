/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
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
