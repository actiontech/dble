package com.actiontech.dble.net.service;


import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.btrace.provider.IODelayProvider;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;


/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService extends VariablesService implements Service {
    private static final Logger LOGGER = LogManager.getLogger(AbstractService.class);
    protected AbstractConnection connection;
    private long firstGraceCloseTime = -1;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
    }

    public AbstractConnection getConnection() {
        return connection;
    }

    public ByteBuffer allocate() {
        return this.connection.allocate();
    }

    public ByteBuffer allocate(int size) {
        return this.connection.allocate(size);
    }


    /**
     * the common method to write to connection.
     *
     * @param buffer
     * @param writeFlags
     */
    public final void writeDirectly(ByteBuffer buffer, @Nonnull EnumSet<WriteFlag> writeFlags) {
        final boolean end = writeFlags.contains(WriteFlag.END_OF_QUERY) || writeFlags.contains(WriteFlag.END_OF_SESSION);
        if (end) {
            beforeWriteFinish(writeFlags);
        }

        this.connection.innerWrite(buffer, writeFlags);
        if (end) {
            afterWriteFinish(writeFlags);
        }

    }

    public void write(byte[] data, @Nonnull EnumSet<WriteFlag> writeFlags) {
        ByteBuffer buffer = connection.allocate();
        ByteBuffer writeBuffer = writeToBuffer(data, buffer);
        this.writeDirectly(writeBuffer, writeFlags);
    }

    /**
     * this method will call writeDirectly method finally.
     *
     * @param packet
     */
    public void write(MySQLPacket packet) {
        packet.bufferWrite(connection);
    }

    protected void beforeWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags) {
        if (writeFlags.contains(WriteFlag.END_OF_QUERY)) {
            TraceManager.sessionFinish(this);
        } else if (writeFlags.contains(WriteFlag.END_OF_SESSION)) {
            TraceManager.sessionFinish(this);
        }
    }


    protected void afterWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags) {

    }


    public void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        buffer = packet.write(buffer, this, true);
        this.writeDirectly(buffer, packet.getLastWriteFlag());
    }

    public void recycleBuffer(ByteBuffer buffer) {
        this.connection.getProcessor().getBufferPool().recycle(buffer);
    }

    private ByteBuffer writeBigPackageToBuffer(byte[] data, ByteBuffer buffer) {
        int srcPos;
        byte[] singlePacket;
        singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
        System.arraycopy(data, 0, singlePacket, 0, MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        srcPos = MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
        int length = data.length;
        length -= (MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
        byte packetId = data[3];
        singlePacket[3] = packetId;
        buffer = writeToBuffer(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = ++packetId;
            if (this instanceof ShardingService) {
                singlePacket[3] = (byte) ((ShardingService) this).nextPacketId();
            }
            System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = writeToBuffer(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = ++packetId;
        if (this instanceof ShardingService) {
            singlePacket[3] = (byte) ((ShardingService) this).nextPacketId();
        }
        System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, length);
        buffer = writeToBuffer(singlePacket, buffer);
        return buffer;
    }


    public boolean isFlowControlled() {
        return this.connection.isFlowControlled();
    }

    public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        return connection.checkWriteBuffer(buffer, capacity, writeSocketIfFull);
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        if (src.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
            return this.writeBigPackageToBuffer(src, buffer);
        }
        int offset = 0;
        int length = src.length;
        int remaining = buffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                buffer.put(src, offset, length);
                break;
            } else {
                buffer.put(src, offset, remaining);
                this.writeDirectly(buffer, WriteFlags.PART);
                buffer = allocate();
                offset += remaining;
                length -= remaining;
                remaining = buffer.remaining();
            }
        }
        return buffer;
    }

    public String toBriefString() {
        return "Service " + this.getClass() + " " + connection.getId();
    }

    protected abstract void handleInnerData(byte[] data);


    public void consumeSingleTask(ServiceTask serviceTask) {
        if (serviceTask.getType() == ServiceTaskType.NORMAL) {
            final byte[] data = ((NormalServiceTask) serviceTask).getOrgData();
            handleInnerData(data);

        } else {
            handleSpecialInnerData((InnerServiceTask) serviceTask);
        }
    }

    protected void handleSpecialInnerData(InnerServiceTask serviceTask) {
        final ServiceTaskType taskType = serviceTask.getType();
        switch (taskType) {
            case CLOSE: {
                IODelayProvider.beforeInnerClose(serviceTask, this);
                final CloseServiceTask task = (CloseServiceTask) serviceTask;
                final Collection<String> closedReasons = task.getReasons();
                if (task.isGracefullyClose()) {
                    if (firstGraceCloseTime == -1) {
                        firstGraceCloseTime = System.currentTimeMillis();
                    }
                    connection.doNextWriteCheck();
                    if (connection.getSocketWR().isWriteComplete()) {
                        connection.closeImmediately(Strings.join(closedReasons, ','));
                        IODelayProvider.afterImmediatelyClose(serviceTask, this);
                    } else {
                        if (System.currentTimeMillis() - firstGraceCloseTime > 10 * 1000) {
                            LOGGER.error("conn graceful close take so long time. {}.", this);
                            connection.closeImmediately(Strings.join(closedReasons, ','));
                            return;
                        }
                        /*
                        delayed, push back to queue.
                         */
                        connection.pushInnerServiceTask(serviceTask);
                    }
                } else {
                    connection.closeImmediately(Strings.join(closedReasons, ','));
                    IODelayProvider.afterImmediatelyClose(serviceTask, this);
                }

            }
            break;
            default:
                LOGGER.info("UNKNOWN INNER COMMAND {} {} for con {}", serviceTask.getClass().getName(), serviceTask, connection);
                break;
        }
    }
}
