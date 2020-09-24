/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.response.PackageBufINf;
import com.actiontech.dble.server.response.ShowTables;
import com.actiontech.dble.server.response.ShowTablesStmtInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;

import java.util.*;

/**
 * Created by huqing.yan on 2017/7/20.
 */
public class ShowTablesHandler extends SingleNodeHandler {
    private String showTableSchema;
    private Map<String, String> shardingTablesMap;
    private Item whereItem;
    private List<Field> sourceFields;
    private ShowTablesStmtInfo info;
    private Set<String> metaTablesSet = new HashSet<>();

    public ShowTablesHandler(RouteResultset rrs, NonBlockingSession session, ShowTablesStmtInfo info) {
        super(rrs, session);
        buffer = session.getSource().allocate();
        this.info = info;
        ShardingService shardingService = session.getShardingService();
        String showSchema = info.getSchema();
        if (showSchema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        showTableSchema = showSchema == null ? shardingService.getSchema() : showSchema;
        SchemaMeta schemata = ProxyMeta.getInstance().getTmManager().getCatalogs().get(showTableSchema);
        if (schemata != null) {
            metaTablesSet.addAll(schemata.getTableMetas().keySet());
        }
        shardingTablesMap = ShowTables.getTableSet(showTableSchema, info);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        ShardingService shardingService = session.getShardingService();
        PackageBufINf bufInf;
        lock.lock();
        try {
            if (writeToClient.get()) {
                return;
            }
            String schemaColumn = showTableSchema;
            this.fieldCount = fields.size();
            if (info.getLike() != null) {
                schemaColumn = schemaColumn + " (" + info.getLike() + ")";
            }
            if (info.isFull()) {
                List<FieldPacket> fieldPackets = new ArrayList<>(2);
                bufInf = ShowTables.writeFullTablesHeader(buffer, shardingService, schemaColumn, fieldPackets);
                buffer = bufInf.getBuffer();
                if (info.getWhere() != null) {
                    MySQLItemVisitor mev = new MySQLItemVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), shardingService.getUsrVariables());
                    info.getWhereExpr().accept(mev);
                    sourceFields = HandlerTool.createFields(fieldPackets);
                    whereItem = HandlerTool.createItem(mev.getItem(), sourceFields, 0, false, DMLResponseHandler.HandlerType.WHERE);
                    bufInf = ShowTables.writeFullTablesRow(buffer, shardingService, shardingTablesMap, whereItem, sourceFields);
                    buffer = bufInf.getBuffer();
                } else {
                    bufInf = ShowTables.writeFullTablesRow(buffer, shardingService, shardingTablesMap, null, null);
                    buffer = bufInf.getBuffer();
                }
            } else {
                bufInf = ShowTables.writeTablesHeaderAndRows(buffer, shardingService, shardingTablesMap, schemaColumn);
                buffer = bufInf.getBuffer();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        RowDataPacket rowDataPacket = new RowDataPacket(1);
        rowDataPacket.read(row);
        String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), session.getShardingService().getCharset().getResults());
        if (shardingTablesMap.containsKey(table)) {
            this.netOutBytes += row.length;
            this.selectRows++;
        } else if (metaTablesSet.contains(table)) { // only show table in meta
            if (whereItem != null) {
                RowDataPacket rowDataPk = new RowDataPacket(sourceFields.size());
                rowDataPk.read(row);
                HandlerTool.initFields(sourceFields, rowDataPk.fieldValues);
                /* filter the where condition */
                if (whereItem.valBool()) {
                    super.rowResponse(row, rowPacket, isLeft, service);
                }
            } else {
                super.rowResponse(row, rowPacket, isLeft, service);
            }
        }
        return false;
    }
}
