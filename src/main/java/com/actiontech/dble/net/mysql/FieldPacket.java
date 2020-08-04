/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;

import java.nio.ByteBuffer;

/**
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * <p>
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 *
 * </pre>
 *
 * @author mycat
 */
public class FieldPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    public static final byte[] DEFAULT_VALUE = new byte[]{(byte) 0x00};
    private static final byte[] FILLER = new byte[2];

    private byte[] catalog = DEFAULT_CATALOG;
    private byte[] db;
    private byte[] table;
    private byte[] orgTable;
    private byte[] name;
    private byte[] orgName;
    private int charsetIndex;
    private long length;
    private int type;
    private int flags;
    private byte decimals;
    private byte[] defaultVal;

    /**
     * change data to FieldPacket
     */
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        this.packetLength = mm.readUB3();
        this.packetId = mm.read();
        readBody(mm);
    }

    /**
     * read BinaryPacket, change to FieldPacket
     */
    public void read(BinaryPacket bin) {
        this.packetLength = bin.packetLength;
        this.packetId = bin.packetId;
        readBody(new MySQLMessage(bin.getData()));
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, AbstractService service,
                            boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = service.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size,
                writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        writeBody(buffer);
        return buffer;
    }

    @Override
    public void bufferWrite(AbstractConnection connection) {
    }

    @Override
    public int calcPacketSize() {
        int size = (catalog == null ? 1 : BufferUtil.getLength(catalog));
        size += (db == null ? 1 : BufferUtil.getLength(db));
        size += (table == null ? 1 : BufferUtil.getLength(table));
        size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
        size += (name == null ? 1 : BufferUtil.getLength(name));
        size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
        size += 13; // 1+2+4+1+2+1+2
        if (defaultVal != null) {
            size += BufferUtil.getLength(defaultVal);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Field Packet";
    }

    private void readBody(MySQLMessage mm) {
        this.catalog = mm.readBytesWithLength();
        this.db = mm.readBytesWithLength();
        this.table = mm.readBytesWithLength();
        this.orgTable = mm.readBytesWithLength();
        this.name = mm.readBytesWithLength();
        this.orgName = mm.readBytesWithLength();
        mm.move(1);
        this.charsetIndex = mm.readUB2();
        this.length = mm.readUB4();
        this.type = mm.read() & 0xff;
        this.flags = mm.readUB2();
        this.decimals = mm.read();
        mm.move(FILLER.length);
        if (mm.hasRemaining()) {
            this.defaultVal = mm.readBytesWithLength();
        }
    }

    private void writeBody(ByteBuffer buffer) {
        byte nullVal = 0;
        BufferUtil.writeWithLength(buffer, catalog, nullVal);
        BufferUtil.writeWithLength(buffer, db, nullVal);
        BufferUtil.writeWithLength(buffer, table, nullVal);
        BufferUtil.writeWithLength(buffer, orgTable, nullVal);
        BufferUtil.writeWithLength(buffer, name, nullVal);
        BufferUtil.writeWithLength(buffer, orgName, nullVal);
        buffer.put((byte) 0x0C);
        BufferUtil.writeUB2(buffer, charsetIndex);
        BufferUtil.writeUB4(buffer, length);
        buffer.put((byte) (type & 0xff));
        BufferUtil.writeUB2(buffer, flags);
        buffer.put(decimals);
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x00);
        //buffer.position(buffer.position() + FILLER.length);
        if (defaultVal != null) {
            BufferUtil.writeWithLength(buffer, defaultVal);
        }
    }

    public byte[] getCatalog() {
        return catalog;
    }

    public void setCatalog(byte[] catalog) {
        this.catalog = catalog;
    }

    public byte[] getDb() {
        return db;
    }

    public void setDb(byte[] db) {
        this.db = db;
    }

    public byte[] getTable() {
        return table;
    }

    public void setTable(byte[] table) {
        this.table = table;
    }

    public byte[] getOrgTable() {
        return orgTable;
    }

    public void setOrgTable(byte[] orgTable) {
        this.orgTable = orgTable;
    }

    public byte[] getName() {
        return name;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getOrgName() {
        return orgName;
    }

    public void setOrgName(byte[] orgName) {
        this.orgName = orgName;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public byte getDecimals() {
        return decimals;
    }

    public void setDecimals(byte decimals) {
        this.decimals = decimals;
    }

    public byte[] getDefaultVal() {
        return defaultVal;
    }

    public void setDefaultVal(byte[] defaultVal) {
        this.defaultVal = defaultVal;
    }
}
