/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.plan.util.ComplexQueryPlanUtil;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.plan.util.ReferenceHandlerInfo;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author mycat
 */
public final class ExplainHandler {
    private ExplainHandler() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainHandler.class);
    private static final int FIELD_COUNT = 3;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("SHARDING_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[2] = PacketUtil.getField("SQL/REF", Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(String stmt, ShardingService service, int offset) {
        stmt = stmt.substring(offset).trim();

        //try to parse the sql again ,stop the inner command
        if (checkInnerCommand(stmt)) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Inner command not route to MySQL:" + stmt);
            return;
        }

        RouteResultset rrs = getRouteResultset(service, stmt);
        if (rrs == null) {
            return;
        }

        writeOutHeadAndEof(service, rrs);
    }

    private static BaseHandlerBuilder buildNodes(RouteResultset rrs, ShardingService service) {
        SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(service.getSchema(), service.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, service.getUsrVariables());
        visitor.visit(ast);
        PlanNode node = visitor.getTableNode();
        node.setSql(rrs.getStatement());
        node.setUpFields();
        PlanUtil.checkTablesPrivilege(service, node, ast);
        node = MyOptimizer.optimize(node);

        if (!PlanUtil.containsSubQuery(node) && !visitor.isContainSchema()) {
            node.setAst(ast);
        }
        HandlerBuilder builder = new HandlerBuilder(node, service.getSession2());
        return builder.getBuilder(service.getSession2(), node, true);
    }

    private static boolean checkInnerCommand(String stmt) {
        int newRes = ServerParse.parse(stmt);
        int sqlType = newRes & 0xff;
        switch (sqlType) {
            case ServerParse.EXPLAIN:
            case ServerParse.EXPLAIN2:
            case ServerParse.KILL:
            case ServerParse.UNLOCK:
            case ServerParse.LOCK:
            case ServerParse.CREATE_VIEW:
            case ServerParse.REPLACE_VIEW:
            case ServerParse.ALTER_VIEW:
            case ServerParse.DROP_VIEW:
            case ServerParse.BEGIN:
            case ServerParse.USE:
            case ServerParse.COMMIT:
            case ServerParse.ROLLBACK:
            case ServerParse.SET:
            case ServerParse.MYSQL_COMMENT:
            case ServerParse.SHOW:
            case ServerParse.SCRIPT_PREPARE:
            case ServerParse.MYSQL_CMD_COMMENT:
            case ServerParse.HELP:
            case ServerParse.LOAD_DATA_INFILE_SQL:
            case ServerParse.FLUSH:
                return true;
            default:
                return false;
        }
    }

    private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(node.getName(), charset));
        row.add(StringUtil.encode("BASE SQL", charset));
        row.add(StringUtil.encode(node.getStatement().replaceAll("[\\t\\n\\r]", " "), charset));
        return row;
    }

    private static RouteResultset getRouteResultset(ShardingService service,
                                                    String stmt) {
        String db = service.getSchema();
        int sqlType = ServerParse.parse(stmt) & 0xff;
        SchemaConfig schema = null;
        if (db != null) {
            schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
            if (schema == null) {
                service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
                return null;
            }
        }
        try {
            if (ServerParse.INSERT == sqlType && isInsertSeq(service, stmt, schema)) {
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "insert sql using sequence,the explain result depends by sequence");
                return null;
            }
            return RouteService.getInstance().route(schema, sqlType, stmt, service, true);
        } catch (Exception e) {
            if (e instanceof SQLException && !(e instanceof SQLNonTransientException)) {
                SQLException sqlException = (SQLException) e;
                StringBuilder s = new StringBuilder();
                LOGGER.info(s.append(service).append(stmt).toString() + " error:" + sqlException);
                String msg = sqlException.getMessage();
                service.writeErrMessage(sqlException.getErrorCode(), msg == null ? sqlException.getClass().getSimpleName() : msg);
                return null;
            } else if (e instanceof SQLSyntaxErrorException) {
                StringBuilder s = new StringBuilder();
                LOGGER.info(s.append(service).append(stmt).toString() + " error:" + e);
                String msg = "druid parse sql error:" + e.getMessage();
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg);
                return null;
            } else {
                StringBuilder s = new StringBuilder();
                LOGGER.warn(s.append(service).append(stmt).toString() + " error:", e);
                String msg = e.getMessage();
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
                return null;
            }
        }
    }

    private static boolean isInsertSeq(ShardingService service, String stmt, SchemaConfig schema) throws SQLException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        MySqlInsertStatement statement = (MySqlInsertStatement) parser.parseStatement();
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = statement.getTableSource();
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, tableSource);
        String tableName = schemaInfo.getTable();
        schema = schemaInfo.getSchemaConfig();
        BaseTableConfig tableConfig = schema.getTables().get(tableName);
        if (tableConfig != null) {
            if (tableConfig instanceof ShardingTableConfig && ((ShardingTableConfig) tableConfig).getIncrementColumn() != null) {
                return true;
            }
            if (tableConfig instanceof ChildTableConfig && ((ChildTableConfig) tableConfig).getIncrementColumn() != null) {
                return true;
            }
        }
        return false;
    }

    public static void writeOutHeadAndEof(ShardingService service, RouteResultset rrs) {
        ByteBuffer buffer = service.allocate();
        // writeDirectly header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        byte packetId = header.getPacketId();
        buffer = header.write(buffer, service, true);

        // writeDirectly fields
        for (FieldPacket field : FIELDS) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, service, true);
        }

        // writeDirectly eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, service, true);

        if (!rrs.isNeedOptimizer()) {
            // writeDirectly rows
            for (RouteResultsetNode node : rrs.getNodes()) {
                RowDataPacket row = getRow(node, service.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        } else {
            BaseHandlerBuilder builder = buildNodes(rrs, service);
            List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
            for (ReferenceHandlerInfo result : results) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(result.getName(), service.getCharset().getResults()));
                row.add(StringUtil.encode(result.getType(), service.getCharset().getResults()));
                row.add(StringUtil.encode(result.getRefOrSQL(), service.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }
        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }
}
