package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

/**
 * alter table 语句解析
 * @author wang.dw
 *
 */
public class DruidAlterTableParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {

	}

	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
			throws SQLNonTransientException {
		SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema.getName(), alterTable.getTableSource());
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		boolean support = false;
		for (SQLAlterTableItem alterItem : alterTable.getItems()) {
			if (alterItem instanceof SQLAlterTableAddColumn
					|| alterItem instanceof SQLAlterTableAddIndex
					|| alterItem instanceof SQLAlterTableDropIndex
					|| alterItem instanceof SQLAlterTableDropKey
					|| alterItem instanceof MySqlAlterTableChangeColumn
					|| alterItem instanceof MySqlAlterTableModifyColumn
					|| alterItem instanceof SQLAlterTableDropColumnItem
					|| alterItem instanceof SQLAlterTableDropPrimaryKey) {
				support = true;
			} else if (alterItem instanceof SQLAlterTableAddConstraint) {
				SQLConstraint constraint = ((SQLAlterTableAddConstraint) alterItem).getConstraint();
				if (constraint instanceof MySqlPrimaryKey) {
					support = true;
				}
			}
		}
		if (!support) {
			String msg = "THE DDL is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs, ctx.getSql());
	}
}
