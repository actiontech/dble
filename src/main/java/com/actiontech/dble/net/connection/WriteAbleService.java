/*
 * Copyright (C) 2016-2022 ActionTech.
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


    /**
     * NOTICE: this method is not a good practice,may deprecated in the future
     * .It doesn't call beforePacket() method.So you should mark multi-result status by yourself.
     * <p>
     * if you are writing the last packet ,use write(Packet)/writeWithBuffer(Packet) instead.
     * if you are writing the non-last packet, use the writeToBuffer(packet,buffer) is better .
     *
     * @param data
     * @param writeFlags
     */
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
        beforePacket(packet);
        packet.bufferWrite(getConnection());
    }

    default void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        this.writeDirectly(writeToBuffer(packet, buffer), packet.getLastWriteFlag());
    }

    default ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        return getConnection().checkWriteBuffer(buffer, capacity, writeSocketIfFull);
    }

    default void beforePacket(MySQLPacket packet) {

    }

    /**
     * NOTICE: this method is not a good practice for write last packet,may deprecated in the future
     * It doesn't call beforePacket() method.So you should mark multi-result status by yourself.
     * <p>
     * use writeToBuffer(packet,buffer) is better.
     *
     * @param src
     * @param buffer
     */
    ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer);


    default ByteBuffer writeToBuffer(MySQLPacket packet, ByteBuffer buffer) {

        // this must instanceof AbstractService
        assert this instanceof AbstractService;
        beforePacket(packet);
        buffer = packet.write(buffer, (AbstractService) this, true);
        return buffer;
    }


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
