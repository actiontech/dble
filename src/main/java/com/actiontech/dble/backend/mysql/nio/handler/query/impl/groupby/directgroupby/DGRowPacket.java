/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * RowPacket used for group by ,contains the result of aggregate function
 * the result of sum is in front of origin RowPacket
 */
public class DGRowPacket extends RowDataPacket {

    private int sumSize;

    /**
     * store tmp result
     **/
    private Object[] sumTranObjs;

    /**
     * store tmp result size
     **/
    private int[] sumByteSizes;

    public DGRowPacket(RowDataPacket innerRow, int sumSize) {
        this(innerRow.getFieldCount(), sumSize);
        this.addAll(innerRow.fieldValues);
    }

    /**
     * @param fieldCount origin field size
     * @param sumSize    sum size to calc
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
     * inaccurate size
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
        ByteBuffer buffer = DbleServer.getInstance().getBufferPool().allocate(size + PACKET_HEADER_SIZE);
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
        for (int i = 0; i < this.getFieldCount(); i++) {
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
        DbleServer.getInstance().getBufferPool().recycle(buffer);
        return data;
    }

    @Override
    public String getPacketInfo() {
        return "Direct Groupby RowData Packet";
    }
}
