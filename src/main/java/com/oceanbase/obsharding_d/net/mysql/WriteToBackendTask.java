/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.mysql;

import com.oceanbase.obsharding_d.backend.mysql.BufferUtil;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

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
            responseService.writeDirectly(buffer, WriteFlags.QUERY_END);
        } catch (java.nio.BufferOverflowException e1) {
            buffer = responseService.checkWriteBuffer(buffer, MySQLPacket.PACKET_HEADER_SIZE + packet.calcPacketSize(), false);
            BufferUtil.writeUB3(buffer, packet.calcPacketSize());
            buffer.put(packet.packetId);
            buffer.put(packet.getCommand());
            buffer = responseService.writeToBuffer(packet.getArg(), buffer);
            responseService.writeDirectly(buffer, WriteFlags.QUERY_END);
        }
    }

    public MySQLResponseService getService() {
        return service;
    }
}
