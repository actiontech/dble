package com.actiontech.dble.net.service;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService implements Service {
    private static final Logger LOGGER = LogManager.getLogger(AbstractService.class);
    protected AbstractConnection connection;
    private AtomicInteger packetId;

    protected ServiceTask currentTask = null;
    private volatile boolean isSupportCompress = false;
    protected volatile ProtoHandler proto;
    protected final BlockingQueue<ServiceTask> taskQueue;
    private int extraPartOfBigPacketCount = 0;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
        this.proto = new MySQLProtoHandlerImpl();
        this.packetId = new AtomicInteger(0);
        if (this instanceof MySQLResponseService) {
            taskQueue = new LinkedBlockingQueue<>();
        } else {
            taskQueue = new LinkedBlockingQueue<>(2000);
        }
    }

    @Override
    public void handle(ByteBuffer dataBuffer) {
        this.sessionStart();
        boolean hasRemaining = true;
        int offset = 0;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, isSupportCompress);
            switch (result.getCode()) {
                case PART_OF_BIG_PACKET:

                    extraPartOfBigPacketCount++;
                    if (!result.isHasMorePacket()) {
                        connection.readReachEnd();
                        dataBuffer.clear();
                    }

                    break;
                case COMPLETE_PACKET:
                    taskCreate(result.getPacketData());
                    if (!result.isHasMorePacket()) {
                        connection.readReachEnd();
                        dataBuffer.clear();
                    }
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    connection.compactReadBuffer(dataBuffer, result.getOffset());
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    connection.ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    break;
                default:
                    throw new RuntimeException("unknown error when read data");
            }

            hasRemaining = result.isHasMorePacket();
            if (hasRemaining) {
                offset = result.getOffset();
            }
        }
    }

    protected void taskCreate(byte[] packetData) {
        if (packetData == null) {
            return;
        }
        int tmpCount = extraPartOfBigPacketCount;
        if (!isSupportCompress) {
            extraPartOfBigPacketCount = 0;
            handleTask(new ServiceTask(packetData, this, tmpCount));
        } else {
            final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
            List<byte[]> packs = CompressUtil.decompressMysqlPacket(packetData, decompressUnfinishedDataQueue);
            if (decompressUnfinishedDataQueue.isEmpty()) {
                extraPartOfBigPacketCount = 0;
            }
            for (byte[] pack : packs) {
                if (pack.length != 0) {
                    handleTask(new ServiceTask(pack, this, tmpCount));
                }
            }
        }
    }

    public void handleTask(ServiceTask task) {
        if (beforeHandlingTask()) {
            try {
                taskQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            taskToTotalQueue(task);
        }
    }

    protected void taskMultiQueryCreate(byte[] packetData) {
        if (beforeHandlingTask()) {
            ServiceTask task = new ServiceTask(packetData, this, true);
            try {
                taskQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
        clearTaskQueue();
        TraceManager.sessionFinish(this);
    }

    protected void clearTaskQueue() {
        this.taskQueue.clear();
    }

    public void register() throws IOException {

    }

    public void consumerInternalData(ServiceTask task) {
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
        byte currentPacketId = data[3];
        buffer = connection.writeToBuffer0(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = ++currentPacketId;
            if (this instanceof ShardingService) {
                singlePacket[3] = (byte) this.nextPacketId();
            } else if (this instanceof RWSplitService) {
                singlePacket[3] = (byte) this.nextPacketId();
            }
            System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = connection.writeToBuffer0(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = ++currentPacketId;
        if (this instanceof ShardingService) {
            singlePacket[3] = (byte) this.nextPacketId();
        } else if (this instanceof RWSplitService) {
            singlePacket[3] = (byte) this.nextPacketId();
        }
        System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, length);
        buffer = connection.writeToBuffer0(singlePacket, buffer);
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
        if (task == null) {
            throw new IllegalStateException("using null task is illegal");
        }
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
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
            if (currentTask != null) {
                //currentTask is executing.
                taskToPriorityQueue(task);
                return;
            }

            executeTask = taskQueue.peek();
            if (executeTask == null) {
                return;
            }
            if (executeTask != task) {
                //out of order,adjust it.
                taskToPriorityQueue(task);
                return;
            }
            //drop head task of the queue
            taskQueue.poll();
            currentTask = executeTask;
        }

        try {
            byte[] data = executeTask.getOrgData();
            if (data != null && !executeTask.isReuse()) {
                this.setPacketId(executeTask.getLastSequenceId());
            }
            if (data != null && data.length - MySQLPacket.PACKET_HEADER_SIZE >= SystemConfig.getInstance().getMaxPacketSize()) {
                throw new IllegalArgumentException("Packet for query is too large (" + data.length + " > " + SystemConfig.getInstance().getMaxPacketSize() + ").You can change maxPacketSize value in bootstrap.cnf.");
            }
            this.handleInnerData(data);
        } catch (Throwable e) {
            String msg = e.getMessage();
            if (StringUtil.isEmpty(msg)) {
                LOGGER.warn("Maybe occur a bug, please check it.", e);
                msg = e.toString();
            } else {
                LOGGER.warn("There is an error you may need know.", e);
            }
            writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        } finally {
            synchronized (this) {
                currentTask = null;
            }

        }
    }

    protected abstract void handleInnerData(byte[] data);

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


    public void parseErrorPacket(byte[] data, String reason) {
        try {
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.read(data);
            String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
            LOGGER.warn("no handler process the execute packet err,sql error:{},back service:{},from reason:{}", errMsg, this, reason);

        } catch (RuntimeException e) {
            LOGGER.info("error handle error-packet", e);
        }
    }
}
