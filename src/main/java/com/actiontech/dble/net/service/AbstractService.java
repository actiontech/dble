package com.actiontech.dble.net.service;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService implements Service {

    protected AbstractConnection connection;
    private AtomicInteger packetId;

    protected ServiceTask currentTask = null;
    private volatile boolean isSupportCompress = false;
    protected volatile ProtoHandler proto;
    protected final ConcurrentLinkedQueue<ServiceTask> taskQueue;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
        this.proto = new MySQLProtoHandlerImpl();
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.packetId = new AtomicInteger(0);
    }

    @Override
    public void handle(ByteBuffer dataBuffer) {
        this.sessionStart();
        boolean hasReming = true;
        int offset = 0;
        while (hasReming) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, isSupportCompress);
            switch (result.getCode()) {
                case REACH_END_BUFFER:
                    connection.readReachEnd();
                    byte[] packetData = result.getPacketData();
                    if (packetData != null) {
                        taskCreate(packetData);
                    }
                    dataBuffer.clear();
                    hasReming = false;
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    connection.compactReadBuffer(dataBuffer, result.getOffset());
                    hasReming = false;
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    connection.ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    hasReming = false;
                    break;
                case STLL_DATA_REMING:
                    byte[] partData = result.getPacketData();
                    if (partData != null) {
                        taskCreate(partData);
                    }
                    offset = result.getOffset();
                    continue;
                default:
                    throw new RuntimeException("unknown error when read data");
            }
        }
    }

    protected void taskCreate(byte[] packetData) {
        if (beforeHandlingTask()) {
            ServiceTask task = new ServiceTask(packetData, this);
            taskQueue.offer(task);
            taskToTotalQueue(task);
        }
    }

    protected void taskMultiQueryCreate(byte[] packetData) {
        if (beforeHandlingTask()) {
            ServiceTask task = new ServiceTask(packetData, this, true);
            taskQueue.offer(task);
            taskToTotalQueue(task);
        }
    }


    protected void sessionStart() {
        //
    }

    @Override
    public void execute(ServiceTask task) {
        task.increasePriority();
        handleData(task);
    }

    public void cleanup() {
        synchronized (this) {
            this.currentTask = null;
        }
        this.taskQueue.clear();
        TraceManager.sessionFinish(this);
    }

    public void register() throws IOException {

    }

    public void consumerInternalData() {
        throw new RuntimeException("function not support");
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
        packet.bufferWrite(connection);
    }

    public void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        buffer = packet.write(buffer, this, true);
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

    public boolean isSupportCompress() {
        return isSupportCompress;
    }

    public void setSupportCompress(boolean supportCompress) {
        isSupportCompress = supportCompress;
    }


    public String toBriefString() {
        return "Service " + this.getClass() + " " + connection.getId();
    }

    protected void taskToPriorityQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
        DbleServer.getInstance().getFrontHandlerQueue().offer(new ServiceTask(null, null));
    }

    protected void taskToTotalQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    protected boolean beforeHandlingTask() {
        return true;
    }

    public void handleData(ServiceTask task) {
        ServiceTask executeTask = null;
        if (connection.isClosed()) {
            return;
        }

        synchronized (this) {
            if (currentTask == null) {
                executeTask = taskQueue.poll();
                if (executeTask != null) {
                    currentTask = executeTask;
                }
            }
            if (currentTask != task) {
                taskToPriorityQueue(task);
            }
        }

        if (executeTask != null) {
            byte[] data = executeTask.getOrgData();
            if (data != null && !executeTask.isReuse()) {
                this.setPacketId(data[3]);
            }
            if (isSupportCompress()) {
                List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, new ConcurrentLinkedQueue<>());
                for (byte[] pack : packs) {
                    if (pack.length != 0) {
                        handleInnerData(pack);
                    }
                }
            } else {
                this.handleInnerData(data);
                synchronized (this) {
                    currentTask = null;
                }
            }
        }
    }

    protected abstract void handleInnerData(byte[] data);

    public void writeOkPacket() {
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
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(id);
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, connection.getCharsetName().getResults()));
        err.setMessage(StringUtil.encode(msg, connection.getCharsetName().getResults()));
        err.write(connection);
    }
}
