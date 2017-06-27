package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * 对SQLStatement解析
 * 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 *  有些只能通过statement解析才能得到所有信息
 *  有些需要通过两种方式解析才能得到完整信息
 * @author wang.dw
 *
 */
public interface DruidParser {
	/**
	 * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
	 * @param schema
	 * @param stmt
	 */
	SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,LayerCachePool cachePool,MycatSchemaStatVisitor schemaStatVisitor) throws SQLException;
	
	/**
	 * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
	 * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
	 * @param stmt
	 */
	SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,MycatSchemaStatVisitor visitor) throws SQLException;
	
	/**
	 * 改写sql：加limit，加group by、加order by如有些没有加limit的可以通过该方法增加
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @throws SQLNonTransientException
	 */
	void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLException;
	/**
	 * 获取解析到的信息
	 * @return
	 */
	DruidShardingParseInfo getCtx();

}
