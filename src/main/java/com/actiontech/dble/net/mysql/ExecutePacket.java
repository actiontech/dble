/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BindValue;
import com.actiontech.dble.backend.mysql.BindValueUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.PreparedStatement;

import java.io.UnsupportedEncodingException;

/**
 * <pre>
 *  Bytes                      Name
 *  -----                      ----
 *  1                          code
 *  4                          statement_id
 *  1                          flags
 *  4                          iteration_count
 *  (param_count+7)/8          null_bit_map
 *  1                          new_parameter_bound_flag (if new_params_bound == 1:)
 *  n*2                        type of parameters
 *  n                          values for the parameters
 *  --------------------------------------------------------------------------------
 *  code:                      always COM_EXECUTE
 *
 *  statement_id:              statement identifier
 *
 *  flags:                     reserved for future use. In MySQL 4.0, always 0.
 *                             In MySQL 5.0:
 *                               0: CURSOR_TYPE_NO_CURSOR
 *                               1: CURSOR_TYPE_READ_ONLY
 *                               2: CURSOR_TYPE_FOR_UPDATE
 *                               4: CURSOR_TYPE_SCROLLABLE
 *
 *  iteration_count:           reserved for future use. Currently always 1.
 *
 *  null_bit_map:              A bitmap indicating parameters that are NULL.
 *                             Bits are counted from LSB, using as many bytes
 *                             as necessary ((param_count+7)/8)
 *                             i.e. if the first parameter (parameter 0) is NULL, then
 *                             the least significant bit in the first byte will be 1.
 *
 *  new_parameter_bound_flag:  Contains 1 if this is the first time
 *                             that "execute" has been called, or if
 *                             the parameters have been rebound.
 *
 *  type:                      Occurs once for each parameter;
 *                             The highest significant bit of this 16-bit value
 *                             encodes the unsigned property. The other 15 bits
 *                             are reserved for the type (only 8 currently used).
 *                             This block is sent when parameters have been rebound
 *                             or when a prepared statement is executed for the
 *                             first time.
 *
 *  values:                    for all non-NULL values, each parameters appends its value
 *                             as described in Row Data Packet: Binary (column values)
 * @see http://dev.mysql.com/doc/internals/en/execute-packet.html
 * </pre>
 *
 * @author mycat
 */
public class ExecutePacket extends MySQLPacket {

    private byte code;
    private long statementId;
    private byte flags;
    private long iterationCount;
    private byte[] nullBitMap;
    private byte newParameterBoundFlag;
    private BindValue[] values;
    private PreparedStatement pStmt;

    public ExecutePacket(PreparedStatement pStmt) {
        this.pStmt = pStmt;
        this.values = new BindValue[pStmt.getParametersNumber()];
    }

    public void read(byte[] data, String charset) throws UnsupportedEncodingException {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        code = mm.read();
        statementId = mm.readUB4();
        flags = mm.read();
        iterationCount = mm.readUB4();

        // read nullBitMap
        int parameterCount = values.length;
        nullBitMap = new byte[(parameterCount + 7) / 8];
        for (int i = 0; i < nullBitMap.length; i++) {
            nullBitMap[i] = mm.read();
        }

        // when newParameterBoundFlag==1,update Parameter type
        newParameterBoundFlag = mm.read();
        if (newParameterBoundFlag == (byte) 1) {
            for (int i = 0; i < parameterCount; i++) {
                pStmt.getParametersType()[i] = mm.readUB2();
            }
        }

        // set Parameter Type and read value
        byte[] bitMap = this.nullBitMap;
        for (int i = 0; i < parameterCount; i++) {
            BindValue bv = new BindValue();
            bv.setType(pStmt.getParametersType()[i]);
            if ((bitMap[i / 8] & (1 << (i & 7))) != 0) {
                bv.setNull(true);
            } else {
                BindValueUtil.read(mm, bv, charset);
                if (bv.isLongData()) {
                    bv.setValue(pStmt.getLongData(i));
                }
            }
            values[i] = bv;
        }
    }

    @Override
    public int calcPacketSize() {

        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Execute Packet";
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public byte getFlags() {
        return flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public long getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(long iterationCount) {
        this.iterationCount = iterationCount;
    }

    public byte[] getNullBitMap() {
        return nullBitMap;
    }

    public void setNullBitMap(byte[] nullBitMap) {
        this.nullBitMap = nullBitMap;
    }

    public byte getNewParameterBoundFlag() {
        return newParameterBoundFlag;
    }

    public void setNewParameterBoundFlag(byte newParameterBoundFlag) {
        this.newParameterBoundFlag = newParameterBoundFlag;
    }

    public BindValue[] getValues() {
        return values;
    }

    public void setValues(BindValue[] values) {
        this.values = values;
    }

    public PreparedStatement getpStmt() {
        return pStmt;
    }

    public void setpStmt(PreparedStatement pStmt) {
        this.pStmt = pStmt;
    }
}
