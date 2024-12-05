/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.visitor.MySQLItemVisitor;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.response.PackageBufINf;
import com.oceanbase.obsharding_d.server.response.ShowTables;
import com.oceanbase.obsharding_d.server.response.ShowTablesStmtInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;

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
        if (showSchema != null && OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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
                                 boolean isLeft, @NotNull AbstractService service) {
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
                    MySQLItemVisitor mev = new MySQLItemVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), shardingService.getUsrVariables(), null);
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
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
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
