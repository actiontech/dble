package io.mycat.backend.mysql.nio.handler.builder;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.PlanNode;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DruidSingleUnitSelectParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.parser.ServerParse;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

public class MergeBuilder {
	private boolean needCommonFlag;
	private boolean needSendMakerFlag;
	private PlanNode node;
	private NonBlockingSession session;
	private MycatConfig config;
	private PushDownVisitor pdVisitor;

	public MergeBuilder(NonBlockingSession session, PlanNode node, boolean needCommon, boolean needSendMaker,
			PushDownVisitor pdVisitor) {
		this.node = node;
		this.needCommonFlag = needCommon;
		this.needSendMakerFlag = needSendMaker;
		this.session = session;
		this.config = MycatServer.getInstance().getConfig();
		this.pdVisitor = pdVisitor;
	}

	/**
	 * 将一个或者多个条件合并后计算出所需要的节点...
	 * 
	 * @return
	 * @throws SQLNonTransientException 
	 * @throws SQLSyntaxErrorException
	 */
	public RouteResultset construct() throws SQLException {
		pdVisitor.visit();
		String sql = pdVisitor.getSql().toString();
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
		MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();
		DruidParser druidParser = new DruidSingleUnitSelectParser();

		RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
		LayerCachePool pool = MycatServer.getInstance().getRouterService().getTableId2DataNodeCache();
		SchemaConfig schemaConfig = config.getSchemas().get(node.getReferedTableNodes().get(0).getSchema());
		return RouterUtil.routeFromParser(druidParser, schemaConfig, rrs, select, sql, pool, visitor, session.getSource());

	}

	/* -------------------- getter/setter -------------------- */
	public boolean getNeedCommonFlag() {
		return needCommonFlag;
	}
	public boolean getNeedSendMakerFlag() {
		return needSendMakerFlag;
	}

}
