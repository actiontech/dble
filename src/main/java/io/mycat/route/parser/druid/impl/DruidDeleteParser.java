package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import io.mycat.MycatServer;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.plan.common.ptr.StringPtr;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * see http://dev.mysql.com/doc/refman/5.7/en/delete.html
 *
 * @author huqing.yan
 */
public class DruidDeleteParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        String schemaName = schema == null ? null : schema.getName();
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        SQLTableSource tableSource = delete.getTableSource();
        SQLTableSource fromSource = delete.getFrom();
        if (fromSource != null) {
            tableSource = fromSource;
        }
        if (tableSource instanceof SQLJoinTableSource) {
            StringPtr sqlSchema = new StringPtr(null);
            if (!SchemaUtil.isNoSharding(sc, (SQLJoinTableSource) tableSource, stmt, schemaName, sqlSchema)) {
                String msg = "DELETE query with multiple tables is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            } else {
                if (delete.getWhere() != null && !SchemaUtil.isNoSharding(sc, delete.getWhere(), schemaName, sqlSchema)) {
                    String msg = "DELETE query with sub-query is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
                SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(realSchema);
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
                RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
                rrs.setFinishedRoute(true);
                return schema;
            }
        } else {
            SQLExprTableSource deleteTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, deleteTableSource);
            if (!MycatPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), Checktype.DELETE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            schema = schemaInfo.getSchemaConfig();
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            if (RouterUtil.isNoSharding(schema, schemaInfo.getTable())) { //整个schema都不分库或者该表不拆分
                if (delete.getWhere() != null && !SchemaUtil.isNoSharding(sc, delete.getWhere(), schemaName, new StringPtr(schemaInfo.getSchema()))) {
                    String msg = "DELETE query with sub-query is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
                return schema;
            }
            super.visitorParse(schema, rrs, stmt, visitor, sc);
            if (visitor.isHasSubQuery()) {
                String msg = "DELETE query with sub-query  is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            TableConfig tc = schema.getTables().get(schemaInfo.getTable());
            if (tc != null && tc.isGlobalTable()) {
                rrs.setGlobalTable(true);
            }
        }
        return schema;
    }
}
