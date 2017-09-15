/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.BindValue;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.PreparedStatement;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.ExecutePacket;
import com.actiontech.dble.net.mysql.LongDataPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.ResetPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.response.PreparedStmtResponse;
import com.actiontech.dble.util.HexFormatUtil;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.escape.Escapers.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat, CrazyPig, zhuam
 */
public class ServerPrepareHandler implements FrontendPrepareHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrepareHandler.class);
    private static Escaper varcharEscaper = null;

    static {
        Builder escapeBuilder = Escapers.builder();
        escapeBuilder.addEscape('\'', "\\'");
        escapeBuilder.addEscape('\\', "\\\\");
        varcharEscaper = escapeBuilder.build();
    }

    private ServerConnection source;
    private volatile long pstmtId;
    private Map<String, PreparedStatement> pstmtForSql;
    private Map<Long, PreparedStatement> pstmtForId;

    public ServerPrepareHandler(ServerConnection source) {
        this.source = source;
        this.pstmtId = 0L;
        this.pstmtForSql = new HashMap<>();
        this.pstmtForId = new HashMap<>();
    }

    @Override
    public void prepare(String sql) {
        LOGGER.debug("use server prepare, sql: " + sql);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForSql.get(sql)) == null) {
            int columnCount = getColumnCount(sql);
            int paramCount = getParamCount(sql);
            pstmt = new PreparedStatement(++pstmtId, sql, columnCount, paramCount);
            pstmtForSql.put(pstmt.getStatement(), pstmt);
            pstmtForId.put(pstmt.getId(), pstmt);
        }
        PreparedStmtResponse.response(pstmt, source);
    }

    @Override
    public void sendLongData(byte[] data) {
        LongDataPacket packet = new LongDataPacket();
        packet.read(data);
        long psId = packet.getPstmtId();
        PreparedStatement pstmt = pstmtForId.get(psId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send long data to prepare sql : " + pstmtForId.get(psId));
            }
            long paramId = packet.getParamId();
            try {
                pstmt.appendLongData(paramId, packet.getLongData());
            } catch (IOException e) {
                source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
            }
        }
    }

    @Override
    public void reset(byte[] data) {
        ResetPacket packet = new ResetPacket();
        packet.read(data);
        long psId = packet.getPstmtId();
        PreparedStatement pstmt = pstmtForId.get(psId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("reset prepare sql : " + pstmtForId.get(psId));
            }
            pstmt.resetLongData();
            source.write(OkPacket.OK);
        } else {
            source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "can not reset prepare statement : " + pstmtForId.get(psId));
        }
    }

    @Override
    public void execute(byte[] data) {
        long psId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForId.get(psId)) == null) {
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset().getClient());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.getValues();
            // reset the Parameter
            String sql = prepareStmtBindValue(pstmt, bindValues);
            source.getSession2().setPrepared(true);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute prepare sql: " + sql);
            }
            source.query(sql);
        }
    }


    @Override
    public void close(byte[] data) {
        long psId = ByteUtil.readUB4(data, 5); // prepare stmt id
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("close prepare stmt, stmtId = " + psId);
        }
        PreparedStatement pstmt = pstmtForId.remove(psId);
        if (pstmt != null) {
            pstmtForSql.remove(pstmt.getStatement());
        }
    }

    @Override
    public void clear() {
        this.pstmtForId.clear();
        this.pstmtForSql.clear();
    }

    // TODO:the size of columns of prepared statement
    private int getColumnCount(String sql) {
        return 0;
    }

    // the size of parameters of prepared statement
    private int getParamCount(String sql) {
        char[] cArr = sql.toCharArray();
        int count = 0;
        for (char aCArr : cArr) {
            if (aCArr == '?') {
                count++;
            }
        }
        return count;
    }

    /**
     * build sql
     *
     * @param pstmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
        String sql = pstmt.getStatement();
        int[] paramTypes = pstmt.getParametersType();
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (int i = 0, len = sql.length(); i < len; i++) {
            char c = sql.charAt(i);
            if (c != '?') {
                sb.append(c);
                continue;
            }
            // execute the ?
            int paramType = paramTypes[idx];
            BindValue bindValue = bindValues[idx];
            idx++;
            // if field is empty
            if (bindValue.isNull()) {
                sb.append("NULL");
                continue;
            }
            switch (paramType & 0xff) {
                case Fields.FIELD_TYPE_TINY:
                    sb.append(String.valueOf(bindValue.getByteBinding()));
                    break;
                case Fields.FIELD_TYPE_SHORT:
                    sb.append(String.valueOf(bindValue.getShortBinding()));
                    break;
                case Fields.FIELD_TYPE_LONG:
                    sb.append(String.valueOf(bindValue.getIntBinding()));
                    break;
                case Fields.FIELD_TYPE_LONGLONG:
                    sb.append(String.valueOf(bindValue.getLongBinding()));
                    break;
                case Fields.FIELD_TYPE_FLOAT:
                    sb.append(String.valueOf(bindValue.getFloatBinding()));
                    break;
                case Fields.FIELD_TYPE_DOUBLE:
                    sb.append(String.valueOf(bindValue.getDoubleBinding()));
                    break;
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                    bindValue.setValue(varcharEscaper.asFunction().apply(String.valueOf(bindValue.getValue())));
                    sb.append("'" + bindValue.getValue() + "'");
                    break;
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_LONG_BLOB:
                    if (bindValue.getValue() instanceof ByteArrayOutputStream) {
                        byte[] bytes = ((ByteArrayOutputStream) bindValue.getValue()).toByteArray();
                        sb.append("X'" + HexFormatUtil.bytesToHexString(bytes) + "'");
                    } else {
                        LOGGER.warn("bind value is not a instance of ByteArrayOutputStream, maybe someone change the implement of long data storage!");
                        sb.append("'" + bindValue.getValue() + "'");
                    }
                    break;
                case Fields.FIELD_TYPE_TIME:
                case Fields.FIELD_TYPE_DATE:
                case Fields.FIELD_TYPE_DATETIME:
                case Fields.FIELD_TYPE_TIMESTAMP:
                    sb.append("'" + bindValue.getValue() + "'");
                    break;
                default:
                    bindValue.setValue(varcharEscaper.asFunction().apply(String.valueOf(bindValue.getValue())));
                    sb.append(bindValue.getValue().toString());
                    break;
            }
        }
        return sb.toString();
    }

}
