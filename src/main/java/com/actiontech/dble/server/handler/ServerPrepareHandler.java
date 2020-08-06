/*
 * Copyright (C) 2016-2020 ActionTech.
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
import com.actiontech.dble.server.response.PreparedStmtResponse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
    private static Escaper varcharEscape = null;

    static {
        Builder escapeBuilder = Escapers.builder();
        escapeBuilder.addEscape('\'', "\\'");
        escapeBuilder.addEscape('\\', "\\\\");
        varcharEscape = escapeBuilder.build();
    }

    private ShardingService service;
    private volatile long pStmtId;
    private Map<Long, PreparedStatement> pStmtForId;

    public ServerPrepareHandler(ShardingService service) {
        this.service = service;
        this.pStmtId = 0L;
        this.pStmtForId = new HashMap<>();
    }

    @Override
    public void prepare(String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("use server prepare, sql: " + sql);
        }
        int columnCount = getColumnCount(sql);
        int paramCount = getParamCount(sql);
        PreparedStatement pStmt = new PreparedStatement(++pStmtId, sql, columnCount, paramCount);
        pStmtForId.put(pStmt.getId(), pStmt);
        PreparedStmtResponse.response(pStmt, service);
    }

    @Override
    public void sendLongData(byte[] data) {
        LongDataPacket packet = new LongDataPacket();
        packet.read(data);
        long psId = packet.getPsStmtId();
        PreparedStatement pStmt = pStmtForId.get(psId);
        if (pStmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send long data to prepare sql : " + pStmtForId.get(psId));
            }
            long paramId = packet.getParamId();
            try {
                pStmt.appendLongData(paramId, packet.getLongData());
            } catch (IOException e) {
                service.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
            }
        }
    }

    @Override
    public void reset(byte[] data) {
        ResetPacket packet = new ResetPacket();
        packet.read(data);
        long psId = packet.getPsStmtId();
        PreparedStatement pStmt = pStmtForId.get(psId);
        if (pStmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("reset prepare sql : " + pStmtForId.get(psId));
            }
            pStmt.resetLongData();
            service.writeDirectly(OkPacket.OK);
        } else {
            service.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "can not reset prepare statement : " + pStmtForId.get(psId));
        }
    }

    @Override
    public void execute(byte[] data) {
        long statementId = ByteUtil.readUB4(data, 5); //skip to read
        PreparedStatement pStmt;
        if ((pStmt = pStmtForId.get(statementId)) == null) {
            service.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pStmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pStmt);
            try {
                packet.read(data, service.getCharset());
            } catch (UnsupportedEncodingException e) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.getValues();
            // reset the Parameter
            String sql = prepareStmtBindValue(pStmt, bindValues);
            service.getSession2().setPrepared(true);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute prepare sql: " + sql);
            }
            pStmt.resetLongData();
            service.query(sql);
        }
    }


    @Override
    public void close(byte[] data) {
        long psId = ByteUtil.readUB4(data, 5); // prepare stmt id
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("close prepare stmt, stmtId = " + psId);
        }
        pStmtForId.remove(psId);
    }

    @Override
    public void clear() {
        this.pStmtForId.clear();
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
     * @param pStmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pStmt, BindValue[] bindValues) {
        String sql = pStmt.getStatement();
        int[] paramTypes = pStmt.getParametersType();
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
                    bindValue.setValue(varcharEscape.asFunction().apply(String.valueOf(bindValue.getValue())));
                    sb.append("'" + bindValue.getValue() + "'");
                    break;
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_LONG_BLOB:
                    if (bindValue.getValue() instanceof ByteArrayOutputStream) {
                        byte[] bytes = ((ByteArrayOutputStream) bindValue.getValue()).toByteArray();
                        sb.append("X'").append(HexFormatUtil.bytesToHexString(bytes)).append("'");
                    } else if (bindValue.getValue() instanceof byte[]) {
                        byte[] bytes = (byte[]) bindValue.getValue();
                        sb.append("X'").append(HexFormatUtil.bytesToHexString(bytes)).append("'");
                    } else {
                        LOGGER.warn("bind value is not a instance of ByteArrayOutputStream,its type is " + bindValue.getValue().getClass());
                        sb.append("'").append(bindValue.getValue().toString()).append("'");
                    }
                    break;
                case Fields.FIELD_TYPE_TIME:
                case Fields.FIELD_TYPE_DATE:
                case Fields.FIELD_TYPE_DATETIME:
                case Fields.FIELD_TYPE_TIMESTAMP:
                    sb.append("'" + bindValue.getValue() + "'");
                    break;
                default:
                    bindValue.setValue(varcharEscape.asFunction().apply(String.valueOf(bindValue.getValue())));
                    sb.append(bindValue.getValue().toString());
                    break;
            }
        }
        return sb.toString();
    }

}
