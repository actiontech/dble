/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ShowTablesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.config.model.sharding.table.SingleTableConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

/**
 * show tables impl
 *
 * @author yanglixue
 */
public final class ShowTables {
    private ShowTables() {
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);

    public static void response(ShardingService shardingService, String stmt) {
        ShowTablesStmtInfo info;
        try {
            info = new ShowTablesStmtInfo(stmt);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            return;
        }
        if (info.getLike() != null && info.getWhere() != null) {
            shardingService.writeErrMessage("42000", "only allow LIKE or WHERE clause in statement", ErrorCode.ER_PARSE_ERROR);
            return;
        }
        String showSchema = info.getSchema();
        if (showSchema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        String cSchema = showSchema == null ? shardingService.getSchema() : showSchema;
        if (cSchema == null) {
            shardingService.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }
        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            shardingService.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(shardingService.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            shardingService.writeErrMessage("42000", "Access denied for user '" + shardingService.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        //if sharding has default node ,show tables will send to backend
        String node = schema.getShardingNode();
        if (!Strings.isNullOrEmpty(node)) {
            try {
                parserAndExecuteShowTables(shardingService, stmt, node, info);
            } catch (Exception e) {
                shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            }
        } else {
            responseDirect(shardingService, cSchema, info);
        }
    }

    private static void parserAndExecuteShowTables(ShardingService shardingService, String originSql, String node, ShowTablesStmtInfo info) throws Exception {
        RouteResultset rrs = new RouteResultset(originSql, ServerParse.SHOW);
        if (info.getSchema() != null || info.isAll()) {
            StringBuilder sql = new StringBuilder();
            sql.append("SHOW ");
            if (info.isFull()) {
                sql.append("FULL ");
            }
            sql.append("TABLES ");
            if (info.getCond() != null) {
                sql.append(info.getCond());
            }
            rrs.setStatement(sql.toString());
        }
        RouterUtil.routeToSingleNode(rrs, node);
        ShowTablesHandler showTablesHandler = new ShowTablesHandler(rrs, shardingService.getSession2(), info);
        showTablesHandler.execute();
    }

    private static void responseDirect(ShardingService shardingService, String cSchema, ShowTablesStmtInfo info) {

        ByteBuffer buffer = shardingService.allocate();
        Map<String, String> tableMap = getTableSet(cSchema, info);
        PackageBufINf bufInf;
        String schemaColumn = cSchema;
        if (info.getLike() != null) {
            schemaColumn = schemaColumn + " (" + info.getLike() + ")";
        }
        if (info.isFull()) {
            List<FieldPacket> fieldPackets = new ArrayList<>(2);
            bufInf = writeFullTablesHeader(buffer, shardingService, schemaColumn, fieldPackets);
            if (info.getWhere() != null) {
                MySQLItemVisitor mev = new MySQLItemVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), shardingService.getUsrVariables());
                info.getWhereExpr().accept(mev);
                List<Field> sourceFields = HandlerTool.createFields(fieldPackets);
                Item whereItem = HandlerTool.createItem(mev.getItem(), sourceFields, 0, false, DMLResponseHandler.HandlerType.WHERE);
                bufInf = writeFullTablesRow(bufInf.getBuffer(), shardingService, tableMap, whereItem, sourceFields);
            } else {
                bufInf = writeFullTablesRow(bufInf.getBuffer(), shardingService, tableMap, null, null);
            }
        } else {
            bufInf = writeTablesHeaderAndRows(buffer, shardingService, tableMap, schemaColumn);
        }
        writeRowEof(bufInf.getBuffer(), shardingService);
    }

    public static PackageBufINf writeFullTablesHeader(ByteBuffer buffer, ShardingService shardingService, String cSchema, List<FieldPacket> fieldPackets) {
        int fieldCount = 2;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        header.setPacketId(shardingService.nextPacketId());
        fields[i] = PacketUtil.getField("Tables_in_" + cSchema, Fields.FIELD_TYPE_VAR_STRING);
        fields[i].setPacketId(shardingService.nextPacketId());
        fieldPackets.add(fields[i]);
        fields[i + 1] = PacketUtil.getField("Table_type", Fields.FIELD_TYPE_VAR_STRING);
        fields[i + 1].setPacketId(shardingService.nextPacketId());
        fieldPackets.add(fields[i + 1]);

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(shardingService.nextPacketId());
        // writeDirectly header
        buffer = header.write(buffer, shardingService, true);
        // writeDirectly fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, shardingService, true);
        }
        eof.write(buffer, shardingService, true);
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        return packBuffInfo;
    }

    public static PackageBufINf writeFullTablesRow(ByteBuffer buffer, ShardingService shardingService, Map<String, String> tableMap, Item whereItem, List<Field> sourceFields) {
        for (Map.Entry<String, String> entry : tableMap.entrySet()) {
            RowDataPacket row = new RowDataPacket(2);
            String name = entry.getKey();
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            row.add(StringUtil.encode(name, shardingService.getCharset().getResults()));
            row.add(StringUtil.encode(entry.getValue(), shardingService.getCharset().getResults()));
            if (whereItem != null) {
                HandlerTool.initFields(sourceFields, row.fieldValues);
                /* filter by where condition */
                if (whereItem.valBool()) {
                    row.setPacketId(shardingService.nextPacketId());
                    buffer = row.write(buffer, shardingService, true);
                }
            } else {
                row.setPacketId(shardingService.nextPacketId());
                buffer = row.write(buffer, shardingService, true);
            }
        }
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        return packBuffInfo;
    }

    public static PackageBufINf writeTablesHeaderAndRows(ByteBuffer buffer, ShardingService shardingService, Map<String, String> tableMap, String cSchema) {
        int fieldCount = 1;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        header.setPacketId(shardingService.nextPacketId());
        fields[i] = PacketUtil.getField("Tables_in_" + cSchema, Fields.FIELD_TYPE_VAR_STRING);
        fields[i].setPacketId(shardingService.nextPacketId());

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(shardingService.nextPacketId());
        // writeDirectly header
        buffer = header.write(buffer, shardingService, true);
        // writeDirectly fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, shardingService, true);
        }
        // writeDirectly eof
        eof.write(buffer, shardingService, true);
        for (String name : tableMap.keySet()) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            row.add(StringUtil.encode(name, shardingService.getCharset().getResults()));
            row.setPacketId(shardingService.nextPacketId());
            buffer = row.write(buffer, shardingService, true);
        }
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        return packBuffInfo;
    }

    private static void writeRowEof(ByteBuffer buffer, ShardingService shardingService) {
        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(shardingService.nextPacketId());
        lastEof.write(buffer, shardingService);
    }

    public static Map<String, String> getTableSet(String cSchema, ShowTablesStmtInfo info) {
        //remove the table which is not created but configured
        SchemaMeta schemata = ProxyMeta.getInstance().getTmManager().getCatalogs().get(cSchema);
        if (schemata == null) {
            return new HashMap<>();
        }
        Map tableMeta = schemata.getTableMetas();
        Map<String, ViewMeta> viewMeta = schemata.getViewMetas();
        TreeMap<String, String> tableMap = new TreeMap<>();
        Pattern pattern = null;
        if (null != info.getLike()) {
            String p = "^" + info.getLike().replaceAll("%", ".*");
            pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
        }
        Map<String, SchemaConfig> schemas = DbleServer.getInstance().getConfig().getSchemas();
        for (BaseTableConfig tbConfig : schemas.get(cSchema).getTables().values()) {
            String tbName = tbConfig.getName();
            if (tableMeta.get(tbName) != null && (pattern == null || pattern.matcher(tbName).matches())) {
                String tbType = "BASE TABLE";
                if (info.isAll()) {
                    if (tbConfig instanceof ShardingTableConfig) {
                        tbType = "SHARDING TABLE";
                    } else if (tbConfig instanceof GlobalTableConfig) {
                        tbType = "GLOBAL TABLE";
                    } else if (tbConfig instanceof SingleTableConfig) {
                        tbType = "SINGLE TABLE";
                    } else {
                        tbType = "CHILD TABLE";
                    }
                }
                tableMap.put(tbName, tbType);
            }
        }
        for (ViewMeta vm : viewMeta.values()) {
            String viewName = vm.getViewName();
            if (viewName != null && (pattern == null || pattern.matcher(viewName).matches())) {
                tableMap.put(viewName, "VIEW");
            }
        }
        return tableMap;
    }

}
