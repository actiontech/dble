package com.actiontech.dble.net.mysql;

import com.actiontech.dble.net.service.AbstractService;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/7/8.
 */
public class EOFRowPacket extends EOFPacket {


    public void write(ByteBuffer buffer, AbstractService service) {
        service.writeWithBuffer(this, buffer);
    }

    public boolean isEndOfQuery() {
        return true;
    }
}
