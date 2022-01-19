/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.WriteFlags;

import java.nio.ByteBuffer;

/**
 * <pre>
 * COM_STMT_CLOSE deallocates a prepared statement
 *
 * No response is sent back to the client.
 *
 * COM_STMT_CLOSE:
 *  Bytes              Name
 *  -----              ----
 *  1                  [19] COM_STMT_CLOSE
 *  4                  statement-id
 *
 *  @see https://dev.mysql.com/doc/internals/en/com-stmt-close.html
 * </pre>
 *
 * @author collapsar
 */
public class PreparedClosePacket extends MySQLPacket {

    public static final byte FIELD_COUNT = 0x19;

    private long statementId;

    public PreparedClosePacket(long statementId) {
        this.statementId = statementId;
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, AbstractService service, boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = service.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(FIELD_COUNT);
        BufferUtil.writeUB4(buffer, statementId);
        return buffer;
    }

    @Override
    public void bufferWrite(AbstractConnection connection) {
        int size = calcPacketSize();
        ByteBuffer buffer = connection.allocate(PACKET_HEADER_SIZE + size);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(FIELD_COUNT);
        BufferUtil.writeUB4(buffer, statementId);
        connection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
    }

    @Override
    public int calcPacketSize() {
        return 5;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Prepared Close Packet";
    }

    @Override
    public boolean isEndOfQuery() {
        return false;
    }

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

}
