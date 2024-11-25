/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ShowTablesHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.GlobalTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.SingleTableConfig;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.visitor.MySQLItemVisitor;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.trace.TraceResult;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
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
        if (showSchema != null && OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        String cSchema = showSchema == null ? shardingService.getSchema() : showSchema;
        if (cSchema == null) {
            shardingService.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }
        SchemaConfig schema = OBsharding_DServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            shardingService.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (OBsharding_DServer.getInstance().getConfig().getUsers().get(shardingService.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            shardingService.writeErrMessage("42000", "Access denied for user '" + shardingService.getUser().getFullName() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        //if sharding has default single node ,show tables will send to backend
        if (schema.isDefaultSingleNode()) {
            String node = schema.getDefaultSingleNode();
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
        RouterUtil.routeToSingleNode(rrs, node, null);
        ShowTablesHandler showTablesHandler = new ShowTablesHandler(rrs, shardingService.getSession2(), info);
        shardingService.getSession2().setPreExecuteEnd(TraceResult.SqlTraceType.SINGLE_NODE_QUERY);
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
                MySQLItemVisitor mev = new MySQLItemVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), shardingService.getUsrVariables(), null);
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
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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
        Map<String, SchemaConfig> schemas = OBsharding_DServer.getInstance().getConfig().getSchemas();
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
