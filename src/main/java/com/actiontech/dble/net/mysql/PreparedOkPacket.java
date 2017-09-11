/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.net.FrontendConnection;

import java.nio.ByteBuffer;

/**
 * <pre>
 * From server to client, in response to prepared statement initialization packet.
 * It is made up of:
 *   1.a PREPARE_OK packet
 *   2.if "number of parameters" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *   3.if "number of columns" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *
 * -----------------------------------------------------------------------------------------
 *
 *  Bytes              Name
 *  -----              ----
 *  1                  0 - marker for OK packet
 *  4                  statement_handler_id
 *  2                  number of columns in result set
 *  2                  number of parameters in query
 *  1                  filler (always 0)
 *  2                  warning count
 *
 *  @see http://dev.mysql.com/doc/internals/en/prepared-statement-initialization-packet.html
 * </pre>
 *
 * @author mycat
 */
public class PreparedOkPacket extends MySQLPacket {

    private byte flag;
    private long statementId;
    private int columnsNumber;
    private int parametersNumber;
    private byte filler;
    private int warningCount;

    public PreparedOkPacket() {
        this.flag = 0;
        this.filler = 0;
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c, boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(flag);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, columnsNumber);
        BufferUtil.writeUB2(buffer, parametersNumber);
        buffer.put(filler);
        BufferUtil.writeUB2(buffer, warningCount);
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return 12;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL PreparedOk Packet";
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public int getColumnsNumber() {
        return columnsNumber;
    }

    public void setColumnsNumber(int columnsNumber) {
        this.columnsNumber = columnsNumber;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    public void setParametersNumber(int parametersNumber) {
        this.parametersNumber = parametersNumber;
    }

    public byte getFiller() {
        return filler;
    }

    public void setFiller(byte filler) {
        this.filler = filler;
    }

    public int getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }
}
