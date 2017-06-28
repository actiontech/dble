package io.mycat.route.impl;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.server.ServerConnection;
import io.mycat.sqlengine.mpp.LoadData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public abstract class AbstractRouteStrategy implements RouteStrategy {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

	@Override
	public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL,
								String charset, ServerConnection sc, LayerCachePool cachePool) throws SQLException {

		RouteResultset rrs = new RouteResultset(origSQL, sqlType);

		/*
		 * 优化debug loaddata输出cache的日志会极大降低性能
		 */
		if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.loadDataHint)) {
			rrs.setCacheAble(false);
		}

		if (schema == null) {
			rrs = routeNormalSqlWithAST(schema, origSQL, rrs, charset, cachePool, sc);
		} else {
			RouteResultset returnedSet = routeSystemInfo(schema, sqlType, origSQL, rrs);
			if (returnedSet == null) {
				rrs = routeNormalSqlWithAST(schema, origSQL, rrs, charset, cachePool, sc);
			}
		}

		return rrs;
	}


	/**
	 * 通过解析AST语法树类来寻找路由
	 */
	public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
														 String charset, LayerCachePool cachePool, ServerConnection sc) throws SQLException;

	/**
	 * 路由信息指令, 如 SHOW、SELECT@@、DESCRIBE
	 */
	public abstract RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType, String stmt, RouteResultset rrs)
			throws SQLException;

	/**
	 * 解析 Show 之类的语句
	 */
	public abstract RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs, String stmt)
			throws SQLException;

}
