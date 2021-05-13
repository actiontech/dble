/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.BindValue;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.PreparedStatement;
import com.actiontech.dble.backend.mysql.store.CursorCache;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.log.general.GeneralLogHelper;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.RequestScope;
import com.actiontech.dble.server.parser.PrepareChangeVisitor;
import com.actiontech.dble.server.parser.PrepareStatementCalculateVisitor;
import com.actiontech.dble.server.response.PreparedStmtResponse;
import com.actiontech.dble.server.variables.OutputStateEnum;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.HexFormatUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.escape.Escapers.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.actiontech.dble.net.mysql.StatusFlags.SERVER_STATUS_CURSOR_EXISTS;
import static com.alibaba.druid.util.JdbcConstants.MYSQL;

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
        this.pStmtForId = new ConcurrentHashMap<>();
    }

    @Override
    public void prepare(String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("use server prepare, sql: " + sql);
        }

        final List<SQLStatement> statements = SQLUtils.parseStatements(sql, MYSQL, true);
        if (statements.isEmpty()) {
            service.writeErrMessage(ErrorCode.ERR_WRONG_USED, "can't parse sql into statement");
            return;
        }
        if (statements.size() > 1) {
            service.writeErrMessage(ErrorCode.ERR_WRONG_USED, "can't use more than one statement in prepare-statement");
            return;
        }
        final SQLStatement sqlStatement = statements.get(0);

        int paramCount = getParamCount(sqlStatement);
        PreparedStatement pStmt = new PreparedStatement(++pStmtId, sql, paramCount);
        final RequestScope requestScope = service.getRequestScope();
        service.getRequestScope().setCurrentPreparedStatement(pStmt);
        service.getRequestScope().setPrepared(true);
        pStmtForId.put(pStmt.getId(), pStmt);
        if (!(sqlStatement instanceof SQLSelectStatement)) {
            //notSelect
            PreparedStmtResponse.response(pStmt, service);
        } else {
            //isSelect,should calculate column count to support cursor if possible.
            final PrepareChangeVisitor visitor = new PrepareChangeVisitor();
            sqlStatement.accept(visitor);
            requestScope.setOutputState(OutputStateEnum.PREPARE);
            requestScope.getCurrentPreparedStatement().setPrepareCallback((columnCount) -> {
                pStmt.setColumnsNumber(columnCount);
                PreparedStmtResponse.response(pStmt, service);
            });
            service.query(sqlStatement.toString());
        }


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
            service.getRequestScope().setCurrentPreparedStatement(pStmt);
            service.getRequestScope().setPrepared(true);
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
            GeneralLogHelper.putGLog(service, MySQLPacket.TO_STRING.get(data[4]), sql);
            final boolean usingCursor = packet.getFlag() == CursorTypeFlags.CURSOR_TYPE_READ_ONLY;
            if (usingCursor) {
                service.getRequestScope().setUsingCursor(true);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute prepare sql: " + sql + ". usingCursor:" + usingCursor);
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
        final PreparedStatement preparedStatement = pStmtForId.get(psId);
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
        pStmtForId.remove(psId);
    }

    @Override
    public void clear() {
        for (PreparedStatement preparedStatement : this.pStmtForId.values()) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                LOGGER.error("", e);
            }

        }
        this.pStmtForId.clear();
    }

    private int getColumnCount(String sql) {
        throw new UnsupportedOperationException();
    }


    // the size of parameters of prepared statement
    private int getParamCount(SQLStatement statement) {
        final PrepareStatementCalculateVisitor visitor = new PrepareStatementCalculateVisitor();
        statement.accept(visitor);
        return visitor.getArgumentCount();
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

    @Override
    public void fetch(byte[] data) {

        long statementId = ByteUtil.readUB4(data, 4 + 1); //skip to read
        PreparedStatement pStmt;
        if ((pStmt = pStmtForId.get(statementId)) == null) {
            service.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pStmtId when executing.");
        } else {

            service.getRequestScope().setCurrentPreparedStatement(pStmt);
            service.getRequestScope().setPrepared(true);

            long expectSize = ByteUtil.readUB4(data, 4 + 1 + 4);
            final CursorCache cursorCache = pStmt.getCursorCache();
            final List<FieldPacket> fieldPackets = service.getRequestScope().getCurrentPreparedStatement().getFieldPackets();
            ByteBuffer buffer = service.getSession2().getSource().allocate();
            try {
                int packetId = 1;
                final Iterator<RowDataPacket> rowDataPacketIt = cursorCache.fetchBatch(expectSize);
                while (rowDataPacketIt.hasNext()) {
                    final RowDataPacket dataPacket = rowDataPacketIt.next();
                    BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                    binRowDataPk.read(fieldPackets, dataPacket);
                    binRowDataPk.setPacketId(packetId++);
                    buffer = binRowDataPk.write(buffer, service, true);
                }
                if (packetId == 1) {
                    /*
                    no more rows
                     */
                    try {
                        pStmt.close();
                    } catch (Exception e) {
                        LOGGER.error("", e);
                    }
                }


                EOFPacket ok = new EOFPacket();
                ok.setPacketId(packetId++);

                //            ok.setAffectedRows(0);
                //            ok.setInsertId(0);
                int statusFlag = 0;
                statusFlag |= service.getSession2().getShardingService().isAutocommit() ? 2 : 1;
                statusFlag |= SERVER_STATUS_CURSOR_EXISTS;
                ok.setStatus(statusFlag);
                ok.setWarningCount(0);
                ok.write(buffer, service, true);
                service.writeDirectly(buffer);


            } finally {
                service.getSession2().getSource().recycle(buffer);
            }

        }
    }


}
