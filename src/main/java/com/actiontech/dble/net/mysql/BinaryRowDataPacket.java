/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;


import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.util.ByteUtil;
import com.actiontech.dble.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * ProtocolBinary::ResultsetRow:
 * row of a binary resultset (COM_STMT_EXECUTE)
 * <p>
 * Payload
 * 1              packet header [00]
 * string[$len]   NULL-bitmap, length: (column_count + 7 + 2) / 8
 * string[$len]   values
 * <p>
 * A Binary Protocol Resultset Row is made up of the NULL bitmap
 * containing as many bits as we have columns in the resultset + 2
 * and the values for columns that are not NULL in the Binary Protocol Value format.
 *
 * @author CrazyPig
 * @see @http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html#packet-ProtocolBinary::ResultsetRow
 * @see @http://dev.mysql.com/doc/internals/en/binary-protocol-value.html
 */
public class BinaryRowDataPacket extends MySQLPacket {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryRowDataPacket.class);
    private int fieldCount;
    private List<byte[]> fieldValues;
    private byte packetHeader = (byte) 0;
    private byte[] nullBitMap;

    private List<FieldPacket> fieldPackets;

    public BinaryRowDataPacket() {
    }

    /**
     * transfor from RowDataPacket to BinaryRowDataPacket
     *
     * @param fields
     * @param rowDataPk
     */
    public void read(List<FieldPacket> fields, RowDataPacket rowDataPk) {
        this.fieldPackets = fields;
        this.fieldCount = rowDataPk.getFieldCount();
        this.fieldValues = new ArrayList<>(fieldCount);
        this.nullBitMap = new byte[(fieldCount + 7 + 2) / 8];

        List<byte[]> values = rowDataPk.fieldValues;
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = values.get(i);
            FieldPacket fieldPk = fields.get(i);
            if (fv == null) {
                storeNullBitMap(i);
                this.fieldValues.add(fv);
            } else {
                convert(fv, fieldPk);
            }
        }
    }

    private void storeNullBitMap(int i) {
        int bitMapPos = (i + 2) / 8;
        int bitPos = (i + 2) % 8;
        this.nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
    }

    /**
     * transform from RowDataPacket's fieldValue to BinaryRowDataPacket's fieldValue
     *
     * @param fv
     * @param fieldPk
     */
    private void convert(byte[] fv, FieldPacket fieldPk) {

        int fieldType = fieldPk.getType();
        switch (fieldType) {
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
            case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_ENUM:
            case Fields.FIELD_TYPE_SET:
            case Fields.FIELD_TYPE_LONG_BLOB:
            case Fields.FIELD_TYPE_MEDIUM_BLOB:
            case Fields.FIELD_TYPE_BLOB:
            case Fields.FIELD_TYPE_TINY_BLOB:
            case Fields.FIELD_TYPE_GEOMETRY:
            case Fields.FIELD_TYPE_BIT:
            case Fields.FIELD_TYPE_DECIMAL:
            case Fields.FIELD_TYPE_NEW_DECIMAL:
                // Fields
                // value (lenenc_str) -- string

                // Example
                // 03 66 6f 6f -- string = "foo"
                this.fieldValues.add(fv);
                break;
            case Fields.FIELD_TYPE_LONGLONG:
                // Fields
                // value (8) -- integer

                // Example
                // 01 00 00 00 00 00 00 00 -- int64 = 1
                long longVar = ByteUtil.getLong(fv);
                this.fieldValues.add(ByteUtil.getBytes(longVar));
                break;
            case Fields.FIELD_TYPE_LONG:
            case Fields.FIELD_TYPE_INT24:
                // Fields
                // value (4) -- integer

                // Example
                // 01 00 00 00 -- int32 = 1
                int intVar = ByteUtil.getInt(fv);
                this.fieldValues.add(ByteUtil.getBytes(intVar));
                break;
            case Fields.FIELD_TYPE_SHORT:
            case Fields.FIELD_TYPE_YEAR:
                // Fields
                // value (2) -- integer

                // Example
                // 01 00 -- int16 = 1
                short shortVar = ByteUtil.getShort(fv);
                this.fieldValues.add(ByteUtil.getBytes(shortVar));
                break;
            case Fields.FIELD_TYPE_TINY:
                // Fields
                // value (1) -- integer

                // Example
                // 01 -- int8 = 1
                int tinyVar = ByteUtil.getInt(fv);
                byte[] bytes = new byte[1];
                bytes[0] = (byte) tinyVar;
                this.fieldValues.add(bytes);
                break;
            case Fields.FIELD_TYPE_DOUBLE:
                // Fields
                // value (string.fix_len) -- (len=8) double

                // Example
                // 66 66 66 66 66 66 24 40 -- double = 10.2
                double doubleVar = ByteUtil.getDouble(fv);
                this.fieldValues.add(ByteUtil.getBytes(doubleVar));
                break;
            case Fields.FIELD_TYPE_FLOAT:
                // Fields
                // value (string.fix_len) -- (len=4) float

                // Example
                // 33 33 23 41 -- float = 10.2
                float floatVar = ByteUtil.getFloat(fv);
                this.fieldValues.add(ByteUtil.getBytes(floatVar));
                break;
            case Fields.FIELD_TYPE_DATE:
                try {
                    Date dateVar = DateUtil.parseDate(ByteUtil.getDate(fv), DateUtil.DATE_PATTERN_ONLY_DATE);
                    this.fieldValues.add(ByteUtil.getBytes(dateVar, false));
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    // when time is 0000-00-00 00:00:00 , return 1970-01-01 08:00:00.0
                    this.fieldValues.add(ByteUtil.getBytes(new Date(0L), false));
                }
                break;
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
                String dateStr = ByteUtil.getDate(fv);
                Date dateTimeVar = null;
                try {
                    if (dateStr.indexOf(".") > 0) {
                        dateTimeVar = DateUtil.parseDate(dateStr, DateUtil.DATE_PATTERN_FULL);
                        this.fieldValues.add(ByteUtil.getBytes(dateTimeVar, false));
                    } else {
                        dateTimeVar = DateUtil.parseDate(dateStr, DateUtil.DEFAULT_DATE_PATTERN);
                        this.fieldValues.add(ByteUtil.getBytes(dateTimeVar, false));
                    }
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    // when time is 0000-00-00 00:00:00 , return 1970-01-01 08:00:00.0
                    this.fieldValues.add(ByteUtil.getBytes(new Date(0L), false));
                }
                break;
            case Fields.FIELD_TYPE_TIME:
                String timeStr = ByteUtil.getTime(fv);
                Date timeVar = null;
                try {
                    if (timeStr.indexOf(".") > 0) {
                        timeVar = DateUtil.parseDate(timeStr, DateUtil.TIME_PATTERN_FULL);
                        this.fieldValues.add(ByteUtil.getBytes(timeVar, true));
                    } else {
                        timeVar = DateUtil.parseDate(timeStr, DateUtil.DEFAULT_TIME_PATTERN);
                        this.fieldValues.add(ByteUtil.getBytes(timeVar, true));
                    }
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    //when time is 0000-00-00 00:00:00,return 1970-01-01 08:00:00.0
                    this.fieldValues.add(ByteUtil.getBytes(new Date(0L), true));
                }
                break;
            default:
                throw new IllegalArgumentException("Field type is not supported");
        }

    }

    @Override
    public void bufferWrite(AbstractConnection conn) {
        int size = calcPacketSize();
        int totalSize = size + PACKET_HEADER_SIZE;
        ByteBuffer bb = conn.getProcessor().getBufferPool().allocate(totalSize);
        BufferUtil.writeUB3(bb, size);
        bb.put(packetId);
        writeBody(bb);
        conn.write(bb);
    }

    @Override
    public ByteBuffer write(ByteBuffer bb, AbstractService service,
                            boolean writeSocketIfFull) {
        int size = calcPacketSize();
        int totalSize = size + PACKET_HEADER_SIZE;
        boolean isBigPackage = size >= MySQLPacket.MAX_PACKET_SIZE;
        if (isBigPackage) {
            service.writeDirectly(bb);
            ByteBuffer tmpBuffer = service.allocate(totalSize);
            BufferUtil.writeUB3(tmpBuffer, calcPacketSize());
            tmpBuffer.put(packetId--);
            writeBody(tmpBuffer);
            byte[] array = tmpBuffer.array();
            service.recycleBuffer(tmpBuffer);
            ByteBuffer newBuffer = service.allocate();
            return service.writeBigPackageToBuffer(array, newBuffer);
        } else {
            bb = service.checkWriteBuffer(bb, totalSize, writeSocketIfFull);
            BufferUtil.writeUB3(bb, size);
            bb.put(packetId);
            writeBody(bb);
            return bb;
        }
    }

    private void writeBody(ByteBuffer bb) {
        bb.put(packetHeader); // packet header [00]
        bb.put(nullBitMap); // NULL-Bitmap
        for (int i = 0; i < fieldCount; i++) { // values
            byte[] fv = fieldValues.get(i);
            if (fv != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
                int fieldType = fieldPk.getType();
                switch (fieldType) {
                    case Fields.FIELD_TYPE_STRING:
                    case Fields.FIELD_TYPE_VARCHAR:
                    case Fields.FIELD_TYPE_VAR_STRING:
                    case Fields.FIELD_TYPE_ENUM:
                    case Fields.FIELD_TYPE_SET:
                    case Fields.FIELD_TYPE_LONG_BLOB:
                    case Fields.FIELD_TYPE_MEDIUM_BLOB:
                    case Fields.FIELD_TYPE_BLOB:
                    case Fields.FIELD_TYPE_TINY_BLOB:
                    case Fields.FIELD_TYPE_GEOMETRY:
                    case Fields.FIELD_TYPE_BIT:
                    case Fields.FIELD_TYPE_DECIMAL:
                    case Fields.FIELD_TYPE_NEW_DECIMAL:
                        // a byte for length (0 means empty)
                        BufferUtil.writeLength(bb, fv.length);
                        break;
                    default:
                        break;
                }
                if (fv.length > 0) {
                    bb.put(fv);
                }
            }
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        size = size + 1 + nullBitMap.length;
        for (int i = 0, n = fieldValues.size(); i < n; i++) {
            byte[] value = fieldValues.get(i);
            if (value != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
                int fieldType = fieldPk.getType();
                switch (fieldType) {
                    case Fields.FIELD_TYPE_STRING:
                    case Fields.FIELD_TYPE_VARCHAR:
                    case Fields.FIELD_TYPE_VAR_STRING:
                    case Fields.FIELD_TYPE_ENUM:
                    case Fields.FIELD_TYPE_SET:
                    case Fields.FIELD_TYPE_LONG_BLOB:
                    case Fields.FIELD_TYPE_MEDIUM_BLOB:
                    case Fields.FIELD_TYPE_BLOB:
                    case Fields.FIELD_TYPE_TINY_BLOB:
                    case Fields.FIELD_TYPE_GEOMETRY:
                    case Fields.FIELD_TYPE_BIT:
                    case Fields.FIELD_TYPE_DECIMAL:
                    case Fields.FIELD_TYPE_NEW_DECIMAL:
                        /*
                         * To convert a length-encoded integer into its numeric value, check the first byte:
                         * If it is < 0xfb, treat it as a 1-byte integer.
                         * If it is 0xfc, it is followed by a 2-byte integer.
                         * If it is 0xfd, it is followed by a 3-byte integer.
                         * If it is 0xfe, it is followed by a 8-byte integer.
                         *
                         */
                        if (value.length != 0) {
                            size = size + BufferUtil.getLength(value);
                        } else {
                            size = size + 1; //empty string
                        }
                        break;
                    default:
                        size = size + value.length;
                        break;
                }
            }
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary RowData Packet";
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    public List<byte[]> getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(List<byte[]> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public byte getPacketHeader() {
        return packetHeader;
    }

    public void setPacketHeader(byte packetHeader) {
        this.packetHeader = packetHeader;
    }

    public byte[] getNullBitMap() {
        return nullBitMap;
    }

    public void setNullBitMap(byte[] nullBitMap) {
        this.nullBitMap = nullBitMap;
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }

    public void setFieldPackets(List<FieldPacket> fieldPackets) {
        this.fieldPackets = fieldPackets;
    }
}
