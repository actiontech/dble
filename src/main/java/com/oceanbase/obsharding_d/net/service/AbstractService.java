/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;


import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.btrace.provider.IODelayProvider;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.WriteAbleService;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.services.VariablesService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.oceanbase.obsharding_d.singleton.TraceManager;
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
    public void beforeWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags, ResultFlag resultFlag) {
        if (writeFlags.contains(WriteFlag.END_OF_QUERY)) {
            TraceManager.sessionFinish(this);
        } else if (writeFlags.contains(WriteFlag.END_OF_SESSION)) {
            TraceManager.sessionFinish(this);
        }
    }

    @Override
    public void afterWriteFinish(@Nonnull EnumSet<WriteFlag> writeFlags) {

    }


    public abstract BufferPoolRecord.Builder generateBufferRecordBuilder();

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
        buffer = writeToBuffer0(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = ++packetId;
            if (this instanceof ShardingService) {
                singlePacket[3] = (byte) ((ShardingService) this).nextPacketId();
            } else if (this instanceof RWSplitService) {
                singlePacket[3] = (byte) ((RWSplitService) this).nextPacketId();
            }
            System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = writeToBuffer0(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = ++packetId;
        if (this instanceof ShardingService) {
            singlePacket[3] = (byte) ((ShardingService) this).nextPacketId();
        } else if (this instanceof RWSplitService) {
            singlePacket[3] = (byte) ((RWSplitService) this).nextPacketId();
        }
        System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, length);
        buffer = writeToBuffer0(singlePacket, buffer);
        return buffer;
    }


    public boolean isFlowControlled() {
        return this.connection.isFrontWriteFlowControlled();
    }

    @Override
    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        if (src.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
            return this.writeBigPackageToBuffer(src, buffer);
        }
        return writeToBuffer0(src, buffer);
    }

    private ByteBuffer writeToBuffer0(byte[] src, ByteBuffer buffer) {
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
                    final CloseServiceTask closeTask = (CloseServiceTask) task;
                    connection.markPrepareClose(closeTask.getReasonsStr());
                    if (closeTask.getCloseType() == CloseType.READ) {
                        //prevent most of nio repeat create close task.
                        connection.getSocketWR().disableReadForever();
                    }
                    if (closeTask.isFirst() && !connection.isOnlyFrontTcpConnected()) {
                        LOGGER.info("prepare close for conn {},reason [{}]", connection, closeTask.getReasonsStr());
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
                            if (LOGGER.isDebugEnabled())
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
