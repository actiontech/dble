/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.connection;

import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlag;
import com.oceanbase.obsharding_d.services.TransactionService;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * @author dcy
 * Create Date: 2021-05-27
 */
public interface WriteAbleService {
    AbstractConnection getConnection();

    default void writeDirectly(ByteBuffer buffer, @Nonnull EnumSet<WriteFlag> writeFlags) {
        writeDirectly(buffer, writeFlags, ResultFlag.OTHER);
    }

    /**
     * the common method to write to connection.
     *
     * @param buffer
     * @param writeFlags
     */
    default void writeDirectly(ByteBuffer buffer, @Nonnull EnumSet<WriteFlag> writeFlags, ResultFlag resultFlag) {
        beforeWriteFinishPure(writeFlags);
        final boolean end = writeFlags.contains(WriteFlag.END_OF_QUERY) || writeFlags.contains(WriteFlag.END_OF_SESSION);
        if (end) {
            beforeWriteFinish(writeFlags, resultFlag);
        }

        getConnection().innerWrite(buffer, writeFlags);
        if (end) {
            afterWriteFinish(writeFlags);
        }
    }

    default void beforeWriteFinishPure(@Nonnull EnumSet<WriteFlag> writeFlags) {
        if ((writeFlags.contains(WriteFlag.END_OF_QUERY) ||
                writeFlags.contains(WriteFlag.END_OF_SESSION) ||
                writeFlags.contains(WriteFlag.PARK_OF_MULTI_QUERY)) &&
                this instanceof TransactionService) {
            TransactionService service = ((TransactionService) this);
            service.redressControlTx();
            StatisticListener.getInstance().record(service, r -> r.onFrontendSqlEnd());
        }
    }

    void beforeWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags, ResultFlag resultFlag);

    void afterWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags);

    default void write(byte[] data, @Nonnull EnumSet<WriteFlag> writeFlags) {
        write(data, writeFlags, ResultFlag.OTHER);
    }

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
    default void write(byte[] data, @Nonnull EnumSet<WriteFlag> writeFlags, ResultFlag resultFlag) {
        ByteBuffer buffer = getConnection().allocate();
        ByteBuffer writeBuffer = writeToBuffer(data, buffer);
        this.writeDirectly(writeBuffer, writeFlags, resultFlag);
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

    default void write(MySQLPacket packet, AbstractService service) {
        beforePacket(packet);
        packet.bufferWrite(service);
    }

    default void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        this.writeDirectly(writeToBuffer(packet, buffer), packet.getLastWriteFlag(), packet.getResultFlag());
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
