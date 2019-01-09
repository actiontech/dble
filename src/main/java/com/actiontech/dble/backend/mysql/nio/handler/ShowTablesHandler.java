/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.manager.handler.PackageBufINf;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.response.ShowTablesStmtInfo;
import com.actiontech.dble.server.response.ShowTables;
import com.actiontech.dble.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by huqing.yan on 2017/7/20.
 */
public class ShowTablesHandler extends SingleNodeHandler {
    private String showTableSchema;
    private Map<String, String> shardingTablesMap;
    private Item whereItem;
    private List<Field> sourceFields;
    private ShowTablesStmtInfo info;
    public ShowTablesHandler(RouteResultset rrs, NonBlockingSession session, ShowTablesStmtInfo info) {
        super(rrs, session);
        this.info = info;
        ServerConnection source = session.getSource();
        String showSchema = info.getSchema();
        if (showSchema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        showTableSchema = showSchema == null ? source.getSchema() : showSchema;
        shardingTablesMap = ShowTables.getTableSet(showTableSchema, info);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        ServerConnection source = session.getSource();
        buffer = allocBuffer();
        PackageBufINf bufInf;
        if (info.isFull()) {
            List<FieldPacket> fieldPackets = new ArrayList<>(2);
            bufInf = ShowTables.writeFullTablesHeader(buffer, source, showTableSchema, fieldPackets);
            packetId = bufInf.getPacketId();
            buffer = bufInf.getBuffer();
            if (info.getWhere() != null) {
                MySQLItemVisitor mev = new MySQLItemVisitor(source.getSchema(), source.getCharset().getResultsIndex(), DbleServer.getInstance().getTmManager());
                info.getWhereExpr().accept(mev);
                sourceFields = HandlerTool.createFields(fieldPackets);
                whereItem = HandlerTool.createItem(mev.getItem(), sourceFields, 0, false, DMLResponseHandler.HandlerType.WHERE);
                bufInf = ShowTables.writeFullTablesRow(buffer, source, shardingTablesMap, packetId, whereItem, sourceFields);
                packetId = bufInf.getPacketId();
                buffer = bufInf.getBuffer();
            } else {
                bufInf = ShowTables.writeFullTablesRow(buffer, source, shardingTablesMap, packetId, null, null);
                packetId = bufInf.getPacketId();
                buffer = bufInf.getBuffer();
            }
        } else {
            bufInf = ShowTables.writeTablesHeaderAndRows(buffer, source, shardingTablesMap, showTableSchema);
            packetId = bufInf.getPacketId();
            buffer = bufInf.getBuffer();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        RowDataPacket rowDataPacket = new RowDataPacket(1);
        rowDataPacket.read(row);
        String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), session.getSource().getCharset().getResults());
        if (shardingTablesMap.containsKey(table)) {
            this.netOutBytes += row.length;
            this.selectRows++;
        } else {
            if (whereItem != null) {
                RowDataPacket rowDataPk = new RowDataPacket(sourceFields.size());
                rowDataPk.read(row);
                HandlerTool.initFields(sourceFields, rowDataPk.fieldValues);
                /* filter the where condition */
                if (whereItem.valBool()) {
                    super.rowResponse(row, rowPacket, isLeft, conn);
                }
            } else {
                super.rowResponse(row, rowPacket, isLeft, conn);
            }
        }
        return false;
    }
}
