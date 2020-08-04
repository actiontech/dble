/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;

import java.util.List;

/**
 * @author mycat
 */
public interface ResponseHandler {

    /**
     * can't get an valid connection
     *
     * @param e
     * @param attachment
     */
    void connectionError(Throwable e, Object attachment);

    /**
     * execute after acquired an valid connection
     */
    void connectionAcquired(BackendConnection connection);

    /**
     * execute after get an error response
     */
    void errorResponse(byte[] err, AbstractService service);

    /**
     * execute after get an OK response
     */
    void okResponse(byte[] ok, AbstractService service);

    /**
     * execute after get an fieldEof response
     */

    void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                          boolean isLeft, AbstractService service);

    /**
     * execute after get an row response
     */
    boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service);

    /**
     * execute after get an rowEof response
     */
    void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service);

    /**
     * on connection close event
     */
    void connectionClose(AbstractService service, String reason);

}
