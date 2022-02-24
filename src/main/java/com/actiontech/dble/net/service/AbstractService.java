package com.actiontech.dble.net.service;


import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.btrace.provider.IODelayProvider;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.WriteAbleService;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;


/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService extends VariablesService implements Service, WriteAbleService {
    private static final Logger LOGGER = LogManager.getLogger(AbstractService.class);
    protected AbstractConnection connection;
    private boolean fakeClosed = false;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    public AbstractConnection getConnection() {
        return connection;
    }

    public boolean isFakeClosed() {
        return fakeClosed;
    }

    public AbstractService setFakeClosed(boolean fakeClosedTmp) {
        fakeClosed = fakeClosedTmp;
        return this;
    }

    @Override
    public void beforeWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags) {
        if (writeFlags.contains(WriteFlag.END_OF_QUERY)) {
            TraceManager.sessionFinish(this);
        } else if (writeFlags.contains(WriteFlag.END_OF_SESSION)) {
            TraceManager.sessionFinish(this);
        }
    }

    @Override
    public void afterWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags) {

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

    @Override
    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        if (src.length > MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
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
                offset += remaining;
                length -= remaining;
                buffer = allocate(length);
                remaining = buffer.remaining();
            }
        }
        return buffer;
    }

    public String toBriefString() {
        return "Service " + this.getClass() + " " + connection.getId();
    }

    protected abstract void handleInnerData(byte[] data);


    /**
     * before the  task  begin process。
     *
     * @param task
     */
    protected boolean beforeHandlingTask(@Nonnull ServiceTask task) {
        return true;
    }

    protected void afterDispatchTask(@Nonnull ServiceTask task) {

    }

    /**
     * before the  task  enqueue。
     *
     * @param task
     */
    protected void beforeInsertServiceTask(@NotNull ServiceTask task) {

        try {
            switch (task.getType()) {
                case CLOSE:
                    connection.markPrepareClose();
                    final CloseServiceTask closeTask = (CloseServiceTask) task;
                    if (closeTask.getCloseType() == CloseType.READ) {
                        //prevent most of nio repeat create close task.
                        connection.getSocketWR().disableReadForever();
                    }
                    if (closeTask.isFirst() && !connection.isOnlyFrontTcpConnected()) {
                        LOGGER.info("prepare close for conn.conn id {},reason [{}]", connection.getId(), closeTask.getReasonsStr());
                    }

                    break;
                case NORMAL:
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void consumeSingleTask(ServiceTask serviceTask) {
        //The close packet can't be filtered
        if (beforeHandlingTask(serviceTask) || (serviceTask.getType() == ServiceTaskType.CLOSE)) {
            if (serviceTask.getType() == ServiceTaskType.NORMAL) {
                final byte[] data = ((NormalServiceTask) serviceTask).getOrgData();
                handleInnerData(data);

            } else {
                handleSpecialInnerData((InnerServiceTask) serviceTask);
            }
        }

        afterDispatchTask(serviceTask);
    }

    protected void handleSpecialInnerData(InnerServiceTask serviceTask) {
        final ServiceTaskType taskType = serviceTask.getType();
        switch (taskType) {
            case CLOSE: {
                if (connection.isClosed()) {
                    return;
                }
                IODelayProvider.beforeInnerClose(serviceTask, this);
                final CloseServiceTask task = (CloseServiceTask) serviceTask;
                final Collection<String> closedReasons = task.getReasons();
                if (task.isGracefullyClose()) {
                    connection.doNextWriteCheck();
                    if (connection.getSocketWR().isWriteComplete()) {
                        connection.closeImmediately(Strings.join(closedReasons, ';'));
                        IODelayProvider.afterImmediatelyClose(serviceTask, this);
                    } else {
                        if (task.getDelayedTimes() > 20) {
                            LOGGER.warn("conn graceful close take so long time. {}.so force close it.", this);
                            connection.closeImmediately(Strings.join(closedReasons, ';'));
                            return;
                        } else {

                            LOGGER.debug("conn graceful close should delay. {}.", this);
                            task.setDelayedTimes(task.getDelayedTimes() + 1);
                            /*
                            delayed, push back to queue.
                             */
                            connection.pushServiceTask(serviceTask);
                        }
                    }
                } else {
                    connection.closeImmediately(Strings.join(closedReasons, ';'));
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
