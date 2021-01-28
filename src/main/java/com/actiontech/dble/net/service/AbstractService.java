package com.actiontech.dble.net.service;


import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService implements Service {

    protected final AbstractConnection connection;
    private final AtomicInteger packetId;
    private final ProtoHandler proto;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
        this.proto = new MySQLProtoHandlerImpl();
        this.packetId = new AtomicInteger(0);
    }

    @Override
    public void handle(ByteBuffer dataBuffer) {
        this.sessionStart();
        boolean hasRemaining = true;
        int offset = 0;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, isSupportCompress());
            switch (result.getCode()) {
                case REACH_END_BUFFER:
                    connection.readReachEnd();
                    byte[] packetData = result.getPacketData();
                    if (packetData != null) {
                        if (!isSupportCompress()) {
                            handleTask(new ServiceTask(packetData, this));
                        } else {
                            List<byte[]> packs = CompressUtil.decompressMysqlPacket(packetData, new ConcurrentLinkedQueue<>());
                            for (byte[] pack : packs) {
                                if (pack.length != 0) {
                                    handleTask(new ServiceTask(pack, this));
                                }
                            }
                        }
                    }
                    dataBuffer.clear();
                    hasRemaining = false;
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    connection.compactReadBuffer(dataBuffer, result.getOffset());
                    hasRemaining = false;
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    connection.ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    hasRemaining = false;
                    break;
                case STLL_DATA_REMING:
                    byte[] partData = result.getPacketData();
                    if (partData != null) {
                        if (!isSupportCompress()) {
                            handleTask(new ServiceTask(partData, this));
                        } else {
                            List<byte[]> packs = CompressUtil.decompressMysqlPacket(partData, new ConcurrentLinkedQueue<>());
                            for (byte[] pack : packs) {
                                if (pack.length != 0) {
                                    handleTask(new ServiceTask(pack, this));
                                }
                            }
                        }
                    }
                    offset = result.getOffset();
                    continue;
                default:
                    throw new RuntimeException("unknown error when read data");
            }
        }
    }

    protected abstract void handleTask(ServiceTask task);

    protected void sessionStart() {
    }

    public int nextPacketId() {
        return packetId.incrementAndGet();
    }

    public void setPacketId(int packetId) {
        this.packetId.set(packetId);
    }

    public AtomicInteger getPacketId() {
        return packetId;
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

    public void writeDirectly(ByteBuffer buffer) {
        markFinished();
        this.connection.write(buffer);
    }

    public void writeDirectly(byte[] data) {
        ByteBuffer buffer = connection.allocate();
        if (data.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
            ByteBuffer writeBuffer = writeBigPackageToBuffer(data, buffer);
            this.writeDirectly(writeBuffer);
        } else {
            ByteBuffer writeBuffer = writeToBuffer(data, buffer);
            this.writeDirectly(writeBuffer);
        }
    }

    public void write(MySQLPacket packet) {
        if (packet.isEndOfSession() || packet.isEndOfQuery()) {
            TraceManager.sessionFinish(this);
        }
        markFinished();
        packet.bufferWrite(connection);
    }

    public void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        buffer = packet.write(buffer, this, true);
        markFinished();
        connection.write(buffer);
        if (packet.isEndOfSession() || packet.isEndOfQuery()) {
            TraceManager.sessionFinish(this);
        }
    }

    public void recycleBuffer(ByteBuffer buffer) {
        this.connection.getProcessor().getBufferPool().recycle(buffer);
    }

    public ByteBuffer writeBigPackageToBuffer(byte[] data, ByteBuffer buffer) {
        int srcPos;
        byte[] singlePacket;
        singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
        System.arraycopy(data, 0, singlePacket, 0, MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        srcPos = MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
        int length = data.length;
        length -= (MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
        singlePacket[3] = data[3];
        buffer = writeToBuffer(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = (byte) nextPacketId();
            System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = writeToBuffer(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = (byte) nextPacketId();
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
        return connection.writeToBuffer(src, buffer);
    }

    public abstract boolean isSupportCompress();

    public String toBriefString() {
        return "Service " + this.getClass() + " " + connection.getId();
    }

    public void writeOkPacket() {
        markFinished();
        OkPacket ok = new OkPacket();
        byte packet = (byte) this.getPacketId().incrementAndGet();
        ok.read(OkPacket.OK);
        ok.setPacketId(packet);
        write(ok);
    }

    public void writeErrMessage(String code, String msg, int vendorCode) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, code, msg);
    }

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, msg);
    }

    public void writeErrMessage(byte id, int vendorCode, String msg) {
        writeErrMessage(id, vendorCode, "HY000", msg);
    }

    protected void writeErrMessage(byte id, int vendorCode, String sqlState, String msg) {
        Optional.ofNullable(StatisticListener.getInstance().getRecorder(this)).ifPresent(r -> r.onFrontendSqlClose());
        markFinished();
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(id);
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, connection.getCharsetName().getResults()));
        err.setMessage(StringUtil.encode(msg, connection.getCharsetName().getResults()));
        err.write(connection);
    }

    protected void markFinished() {
    }

}
