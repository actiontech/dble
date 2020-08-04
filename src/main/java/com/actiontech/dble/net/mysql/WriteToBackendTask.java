/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.nio.ByteBuffer;

public class WriteToBackendTask {
    private final MySQLResponseService service;
    private final CommandPacket packet;

    public WriteToBackendTask(MySQLResponseService service, CommandPacket packet) {
        this.service = service;
        this.packet = packet;
    }

    public void execute() {
        int size = packet.calcPacketSize();
        if (size >= MySQLPacket.MAX_PACKET_SIZE) {
            packet.writeBigPackage(service, size);
        } else {
            writeCommonPackage(service);
        }
    }

    private void writeCommonPackage(MySQLResponseService responseService) {
        ByteBuffer buffer = responseService.allocate();
        try {
            BufferUtil.writeUB3(buffer, packet.calcPacketSize());
            buffer.put(packet.packetId);
            buffer.put(packet.getCommand());
            buffer = responseService.writeToBuffer(packet.getArg(), buffer);
            responseService.writeDirectly(buffer);
        } catch (java.nio.BufferOverflowException e1) {
            buffer = responseService.checkWriteBuffer(buffer, MySQLPacket.PACKET_HEADER_SIZE + packet.calcPacketSize(), false);
            BufferUtil.writeUB3(buffer, packet.calcPacketSize());
            buffer.put(packet.packetId);
            buffer.put(packet.getCommand());
            buffer = responseService.writeToBuffer(packet.getArg(), buffer);
            responseService.writeDirectly(buffer);
        }
    }
}
