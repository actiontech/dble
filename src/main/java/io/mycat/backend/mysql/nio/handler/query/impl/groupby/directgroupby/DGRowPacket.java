package io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby;


import io.mycat.MycatServer;
import io.mycat.backend.mysql.BufferUtil;
import io.mycat.backend.mysql.ByteUtil;
import io.mycat.net.mysql.RowDataPacket;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * proxy层进行group by计算时用到的RowPacket，比传统的rowpacket多保存了聚合函数的结果
 * sum的结果存放在RowPacket的最前面
 */
public class DGRowPacket extends RowDataPacket {

    private int sumSize;

    /**
     * 保存的中间聚合对象
     **/
    private Object[] sumTranObjs;

    /**
     * 保存中间聚合结果的大小
     **/
    private int[] sumByteSizes;

    public DGRowPacket(RowDataPacket innerRow, int sumSize) {
        this(innerRow.fieldCount, sumSize);
        this.addAll(innerRow.fieldValues);
    }

    /**
     * @param fieldCount 原始的field的个数
     * @param sumSize    要计算的sum的个数
     */
    public DGRowPacket(int fieldCount, int sumSize) {
        super(fieldCount);
        this.sumSize = sumSize;
        sumTranObjs = new Object[sumSize];
        sumByteSizes = new int[sumSize];
    }

    public void setSumTran(int index, Object trans, int transSize) {
        if (index >= sumSize)
            throw new RuntimeException("Set sumTran out of sumSize index!");
        else {
            sumTranObjs[index] = trans;
            sumByteSizes[index] = transSize;
        }
    }

    public Object getSumTran(int index) {
        if (index >= sumSize)
            throw new RuntimeException("Set sumTran out of sumSize index!");
        else {
            return sumTranObjs[index];
        }
    }


    @Override
    /**
     * 提供一个不准确的size
     */
    public int calcPacketSize() {
        int size = super.calcPacketSize();
        for (int i = 0; i < sumSize; i++) {
            int byteSize = sumByteSizes[i];
            size += ByteUtil.decodeLength(byteSize) + byteSize;
        }
        return size;
    }

    private int getRealSize() {
        int size = super.calcPacketSize();
        for (int i = 0; i < sumSize; i++) {
            byte[] v = null;
            Object obj = sumTranObjs[i];
            if (obj != null)
                v = SerializationUtils.serialize((Serializable) obj);
            size += (v == null || v.length == 0) ? 1 : ByteUtil.decodeLength(v);
        }
        return size;
    }

    @Override
    public byte[] toBytes() {
        int size = getRealSize();
        ByteBuffer buffer = MycatServer.getInstance().getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        for (int i = 0; i < this.sumSize; i++) {
            Object obj = sumTranObjs[i];
            byte[] ov = null;
            if (obj != null)
                ov = SerializationUtils.serialize((Serializable) obj);
            if (ov == null) {
                buffer.put(NULL_MARK);
            } else if (ov.length == 0) {
                buffer.put(EMPTY_MARK);
            } else {
                BufferUtil.writeWithLength(buffer, ov);
            }
        }
        for (int i = 0; i < this.fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv == null) {
                buffer.put(NULL_MARK);
            } else if (fv.length == 0) {
                buffer.put(EMPTY_MARK);
            } else {
                BufferUtil.writeWithLength(buffer, fv);
            }
        }
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        MycatServer.getInstance().getBufferPool().recycle(buffer);
        return data;
    }

    @Override
    public String getPacketInfo() {
        return "Direct Groupby RowData Packet";
    }

    public static void main(String[] args) {
        DGRowPacket row = new DGRowPacket(2, 2);
        row.add(new byte[1]);
        row.add(new byte[1]);
        row.setSumTran(0, 1, 4);
        row.setSumTran(1, 2.2, 8);
        byte[] bb = row.toBytes();
        RowDataPacket rp = new RowDataPacket(4);
        rp.read(bb);
        DGRowPacket dgRow = new DGRowPacket(2, 2);
        for (int i = 0; i < 2; i++) {
            byte[] b = rp.getValue(i);
            if (b != null) {
                Object obj = SerializationUtils.deserialize(b);
                dgRow.setSumTran(i, obj, 4);
            }
        }
        for (int i = 2; i < 4; i++) {
            dgRow.add(rp.getValue(i));
        }
    }

}
