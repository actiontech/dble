/*
 * Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.manager.handler.PackageBufINf;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;

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

    public static void response(ServerConnection c, String stmt) {
        ShowTablesStmtInfo info;
        try {
            info = new ShowTablesStmtInfo(stmt);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            return;
        }
        if (info.getLike() != null && info.getWhere() != null) {
            c.writeErrMessage("42000", "only allow LIKE or WHERE clause in statement", ErrorCode.ER_PARSE_ERROR);
            return;
        }
        String showSchema = info.getSchema();
        if (showSchema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        String cSchema = showSchema == null ? c.getSchema() : showSchema;
        if (cSchema == null) {
            c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }
        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            c.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig user = conf.getUsers().get(c.getUser());
        if (user == null || !user.getSchemas().contains(cSchema)) {
            c.writeErrMessage("42000", "Access denied for user '" + c.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        //if schema has default node ,show tables will send to backend
        String node = schema.getDataNode();
        if (!Strings.isNullOrEmpty(node)) {
            try {
                parserAndExecuteShowTables(c, stmt, node, info);
            } catch (Exception e) {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            }
        } else {
            responseDirect(c, cSchema, info);
        }
    }

    private static void parserAndExecuteShowTables(ServerConnection c, String originSql, String node, ShowTablesStmtInfo info) throws Exception {
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
        ShowTablesHandler showTablesHandler = new ShowTablesHandler(rrs, c.getSession2(), info);
        showTablesHandler.execute();
    }

    private static void responseDirect(ServerConnection c, String cSchema, ShowTablesStmtInfo info) {
        ByteBuffer buffer = c.allocate();
        Map<String, String> tableMap = getTableSet(cSchema, info);
        PackageBufINf bufInf;
        if (info.isFull()) {
            List<FieldPacket> fieldPackets = new ArrayList<>(2);
            bufInf = writeFullTablesHeader(buffer, c, cSchema, fieldPackets);
            if (info.getWhere() != null) {
                MySQLItemVisitor mev = new MySQLItemVisitor(c.getSchema(), c.getCharset().getResultsIndex(), DbleServer.getInstance().getTmManager());
                info.getWhereExpr().accept(mev);
                List<Field> sourceFields = HandlerTool.createFields(fieldPackets);
                Item whereItem = HandlerTool.createItem(mev.getItem(), sourceFields, 0, false, DMLResponseHandler.HandlerType.WHERE);
                bufInf = writeFullTablesRow(bufInf.getBuffer(), c, tableMap, bufInf.getPacketId(), whereItem, sourceFields);
            } else {
                bufInf = writeFullTablesRow(bufInf.getBuffer(), c, tableMap, bufInf.getPacketId(), null, null);
            }
        } else {
            bufInf = writeTablesHeaderAndRows(buffer, c, tableMap, cSchema);
        }

        writeRowEof(bufInf.getBuffer(), c, bufInf.getPacketId());
    }

    public static PackageBufINf writeFullTablesHeader(ByteBuffer buffer, ServerConnection c, String cSchema, List<FieldPacket> fieldPackets) {
        int fieldCount = 2;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        byte packetId = 0;
        header.setPacketId(++packetId);
        fields[i] = PacketUtil.getField("Tables in " + cSchema, Fields.FIELD_TYPE_VAR_STRING);
        fields[i].setPacketId(++packetId);
        fieldPackets.add(fields[i]);
        fields[i + 1] = PacketUtil.getField("Table_type  ", Fields.FIELD_TYPE_VAR_STRING);
        fields[i + 1].setPacketId(++packetId);
        fieldPackets.add(fields[i + 1]);

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, c, true);
        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }
        eof.write(buffer, c, true);
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        packBuffInfo.setPacketId(packetId);
        return packBuffInfo;
    }

    public static PackageBufINf writeFullTablesRow(ByteBuffer buffer, ServerConnection c, Map<String, String> tableMap, byte packetId, Item whereItem, List<Field> sourceFields) {
        for (Map.Entry<String, String> entry : tableMap.entrySet()) {
            RowDataPacket row = new RowDataPacket(2);
            String name = entry.getKey();
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            row.add(StringUtil.encode(name, c.getCharset().getResults()));
            row.add(StringUtil.encode(entry.getValue(), c.getCharset().getResults()));
            if (whereItem != null) {
                HandlerTool.initFields(sourceFields, row.fieldValues);
                /* filter by where condition */
                if (whereItem.valBool()) {
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
                }
            } else {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        packBuffInfo.setPacketId(packetId);
        return packBuffInfo;
    }

    public static PackageBufINf writeTablesHeaderAndRows(ByteBuffer buffer, ServerConnection c, Map<String, String> tableMap, String cSchema) {
        int fieldCount = 1;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        byte packetId = 0;
        header.setPacketId(++packetId);
        fields[i] = PacketUtil.getField("Tables in " + cSchema, Fields.FIELD_TYPE_VAR_STRING);
        fields[i].setPacketId(++packetId);

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, c, true);
        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        eof.write(buffer, c, true);
        for (String name : tableMap.keySet()) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            row.add(StringUtil.encode(name, c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        packBuffInfo.setPacketId(packetId);
        return packBuffInfo;
    }

    private static void writeRowEof(ByteBuffer buffer, ServerConnection c, byte packetId) {
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    public static Map<String, String> getTableSet(String cSchema, ShowTablesStmtInfo info) {
        //remove the table which is not created but configured
        SchemaMeta schemata = DbleServer.getInstance().getTmManager().getCatalogs().get(cSchema);
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
        for (TableConfig tbConfig : schemas.get(cSchema).getTables().values()) {
            String tbName = tbConfig.getName();
            if (tableMeta.get(tbName) != null && (pattern == null || pattern.matcher(tbName).matches())) {
                String tbType = "BASE TABLE";
                if (info.isAll()) {
                    tbType = tbConfig.getTableType() == TableConfig.TableTypeEnum.TYPE_GLOBAL_TABLE ? "GLOBAL TABLE" : "SHARDING TABLE";
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
