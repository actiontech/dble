/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;

import java.nio.ByteBuffer;

/**
 * load data local infile
 */
public class RequestFilePacket extends MySQLPacket {
    public static final byte FIELD_COUNT = (byte) 251;
    private byte command = FIELD_COUNT;
    private byte[] fileName;


    @Override
    public ByteBuffer write(ByteBuffer buffer, AbstractService service, boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = service.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(command);
        if (fileName != null) {
            buffer.put(fileName);
        }
        service.writeDirectly(buffer);
        return buffer;
    }

    @Override
    public void bufferWrite(AbstractConnection connection) {

    }

    @Override
    public int calcPacketSize() {
        return fileName == null ? 1 : 1 + fileName.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Request File Packet";
    }


    public byte getCommand() {
        return command;
    }

    public void setCommand(byte command) {
        this.command = command;
    }

    public byte[] getFileName() {
        return fileName;
    }

    public void setFileName(byte[] fileName) {
        this.fileName = fileName;
    }
}
