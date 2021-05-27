/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.connection;

import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.WriteFlag;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * @author dcy
 * Create Date: 2021-05-27
 */
public interface WriteAbleService {
    AbstractConnection getConnection();

    /**
     * the common method to write to connection.
     *
     * @param buffer
     * @param writeFlags
     */
    default void writeDirectly(ByteBuffer buffer, @Nonnull EnumSet<WriteFlag> writeFlags) {
        final boolean end = writeFlags.contains(WriteFlag.END_OF_QUERY) || writeFlags.contains(WriteFlag.END_OF_SESSION);
        if (end) {
            beforeWriteFinish(writeFlags);
        }

        getConnection().innerWrite(buffer, writeFlags);
        if (end) {
            afterWriteFinish(writeFlags);
        }
    }

    void beforeWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags);

    void afterWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags);


    default void write(byte[] data, @Nonnull EnumSet<WriteFlag> writeFlags) {
        ByteBuffer buffer = getConnection().allocate();
        ByteBuffer writeBuffer = writeToBuffer(data, buffer);
        this.writeDirectly(writeBuffer, writeFlags);
    }

    /**
     * this method will call writeDirectly method finally.
     *
     * @param packet
     */
    default void write(MySQLPacket packet) {
        packet.bufferWrite(getConnection());
    }

    default void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        // this must instanceof AbstractService
        assert this instanceof AbstractService;
        buffer = packet.write(buffer, (AbstractService) this, true);
        this.writeDirectly(buffer, packet.getLastWriteFlag());
    }

    default ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        return getConnection().checkWriteBuffer(buffer, capacity, writeSocketIfFull);
    }

    ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer);


    default void recycleBuffer(ByteBuffer buffer) {
        this.getConnection().getProcessor().getBufferPool().recycle(buffer);
    }


    default ByteBuffer allocate() {
        return this.getConnection().allocate();
    }

    default ByteBuffer allocate(int size) {
        return this.getConnection().allocate(size);
    }
}
