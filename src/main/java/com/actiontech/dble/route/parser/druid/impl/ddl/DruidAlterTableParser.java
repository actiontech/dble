/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DruidAlterTableParser
 *
 * @author wang.dw
 */
public class DruidAlterTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, alterTable.getTableSource());
        boolean support = false;
        String msg = "The DDL is not supported, sql:";
        for (SQLAlterTableItem alterItem : alterTable.getItems()) {
            if (alterItem instanceof SQLAlterTableAddColumn ||
                    alterItem instanceof SQLAlterTableDropKey ||
                    alterItem instanceof SQLAlterTableDropPrimaryKey) {
                support = true;
            } else if (alterItem instanceof SQLAlterTableAddIndex ||
                    alterItem instanceof SQLAlterTableDropIndex ||
                    alterItem instanceof MySqlAlterTableAlterColumn) {
                support = true;
                rrs.setOnline(true);
            } else if (alterItem instanceof SQLAlterTableAddConstraint) {
                SQLConstraint constraint = ((SQLAlterTableAddConstraint) alterItem).getConstraint();
                if (constraint instanceof MySqlPrimaryKey) {
                    support = true;
                }
            } else if (alterItem instanceof MySqlAlterTableChangeColumn ||
                    alterItem instanceof MySqlAlterTableModifyColumn ||
                    alterItem instanceof SQLAlterTableDropColumnItem) {
                List<SQLName> columnList = new ArrayList<>();
                if (alterItem instanceof MySqlAlterTableChangeColumn) {
                    columnList.add(((MySqlAlterTableChangeColumn) alterItem).getColumnName());
                } else if (alterItem instanceof MySqlAlterTableModifyColumn) {
                    columnList.add(((MySqlAlterTableModifyColumn) alterItem).getNewColumnDefinition().getName());
                } else if (alterItem instanceof SQLAlterTableDropColumnItem) {
                    columnList = ((SQLAlterTableDropColumnItem) alterItem).getColumns();
                }
                support = !this.columnInfluenceCheck(columnList, schemaInfo.getSchemaConfig(), schemaInfo.getTable());
                if (!support) {
                    msg = "The columns may be sharding keys or ER keys, are not allowed to alter sql:";
                }
            } else if (supportTableOption(alterItem)) {
                support = true;
            }
        }
        if (!support) {
            msg = msg + stmt;
            throw new SQLNonTransientException(msg);
        }
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
            return schemaInfo.getSchemaConfig();
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.getSchemaConfig();
    }

    private boolean supportTableOption(SQLAlterTableItem alterItem) {
        if (null == alterItem || !(alterItem instanceof MySqlAlterTableOption)) {
            return false;
        }
        MySqlAlterTableOption mySqlAlterTableOption = (MySqlAlterTableOption) alterItem;
        String name = mySqlAlterTableOption.getName();
        switch (name) {
            case "COMMENT":
            case "comment":
                return true;
            default:
                return false;
        }
    }


    /**
     * the function is check if the columns contains the import column
     * true -- yes the sql did not to exec
     * false -- safe the sql can be exec
     */
    private boolean columnInfluenceCheck(List<SQLName> columnList, SchemaConfig schema, String table) {
        for (SQLName name : columnList) {
            if (this.influenceKeyColumn(name, schema, table)) {
                return true;
            }
        }
        return false;
    }

    /**
     * this function is check if the name is the important column in any tables
     * true -- the column influence some important column
     * false -- safe
     */
    private boolean influenceKeyColumn(SQLName name, SchemaConfig schema, String tableName) {
        String columnName = name.toString();
        Map<String, BaseTableConfig> tableConfig = schema.getTables();
        BaseTableConfig changedTable = tableConfig.get(tableName);
        if (changedTable == null) {
            return false;
        }
        if (changedTable instanceof ShardingTableConfig &&
                columnName.equalsIgnoreCase(((ShardingTableConfig) changedTable).getShardingColumn())) {
            return true;
        }
        if (changedTable instanceof ChildTableConfig &&
                columnName.equalsIgnoreCase(((ChildTableConfig) changedTable).getJoinColumn())) {
            return true;
        }
        // Traversal all the table node to find if some table is the child table of the changedTale
        for (Map.Entry<String, BaseTableConfig> entry : tableConfig.entrySet()) {
            BaseTableConfig tb = entry.getValue();
            if (tb instanceof ChildTableConfig) {
                ChildTableConfig childTable = (ChildTableConfig) tb;
                if (childTable.getParentTC() != null &&
                        tableName.equalsIgnoreCase(childTable.getParentTC().getName()) &&
                        columnName.equalsIgnoreCase(childTable.getParentColumn())) {
                    return true;
                }
            }
        }
        return false;
    }

}
