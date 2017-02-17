package io.mycat.route.parser.druid;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.parser.druid.impl.DruidDeleteParser;
import io.mycat.route.parser.druid.impl.DruidInsertParser;
import io.mycat.route.parser.druid.impl.DruidLockTableParser;
import io.mycat.route.parser.druid.impl.DruidSelectParser;
import io.mycat.route.parser.druid.impl.DruidUpdateParser;
import io.mycat.route.parser.druid.impl.ddl.DruidAlterTableParser;
import io.mycat.route.parser.druid.impl.ddl.DruidCreateIndexParser;
import io.mycat.route.parser.druid.impl.ddl.DruidCreateTableParser;
import io.mycat.route.parser.druid.impl.ddl.DruidDropIndexParser;
import io.mycat.route.parser.druid.impl.ddl.DruidDropTableParser;
import io.mycat.route.parser.druid.impl.ddl.DruidTruncateTableParser;

/**
 * DruidParser的工厂类
 *
 * @author wdw
 */
public class DruidParserFactory
{
	public static DruidParser create(SQLStatement statement)
			throws SQLNonTransientException {
		DruidParser parser = null;
		if (statement instanceof SQLSelectStatement) {
			parser = new DruidSelectParser();
		} else if (statement instanceof MySqlInsertStatement) {
			parser = new DruidInsertParser();
		} else if (statement instanceof MySqlDeleteStatement) {
			parser = new DruidDeleteParser();
		} else if (statement instanceof MySqlUpdateStatement) {
			parser = new DruidUpdateParser();
		} else if (statement instanceof MySqlLockTableStatement) {
			parser = new DruidLockTableParser();
		} else if (statement instanceof SQLDDLStatement) {
			if (statement instanceof MySqlCreateTableStatement) {
				parser = new DruidCreateTableParser();
			} else if (statement instanceof SQLDropTableStatement) {
				parser = new DruidDropTableParser();
			} else if (statement instanceof SQLAlterTableStatement) {
				parser = new DruidAlterTableParser();
			} else if (statement instanceof SQLTruncateStatement) {
				parser = new DruidTruncateTableParser();
			} else if (statement instanceof SQLCreateIndexStatement) {
				parser = new DruidCreateIndexParser();
			} else if (statement instanceof SQLDropIndexStatement) {
				parser = new DruidDropIndexParser();
			} else {
				String msg = "THE DLL is not supported :" + statement;
				throw new SQLNonTransientException(msg);
			}
		} else {
			parser = new DefaultDruidParser();
		}

		return parser;
	}
}
