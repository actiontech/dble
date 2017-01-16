package io.mycat.route.util;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.wall.spi.WallVisitorUtils;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.SessionSQLPair;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.LoadData;
import io.mycat.util.StringUtil;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * @author wang.dw
 *
 */
public class RouterUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);
	
	/**
	 * 移除执行语句中的数据库名
	 *
	 * @param stmt		 执行语句
	 * @param schema  	数据库名
	 * @return 			执行语句
	 * 
	 * @author mycat
	 */
	public static String removeSchema(String stmt, String schema) {
		final String upStmt = stmt.toUpperCase();
		final String upSchema = schema.toUpperCase() + ".";
		int strtPos = 0;
		int indx = 0;
		boolean flag = false;
		indx = upStmt.indexOf(upSchema, strtPos);
		if (indx < 0) {
			StringBuilder sb = new StringBuilder("`").append(
					schema.toUpperCase()).append("`.");
			indx = upStmt.indexOf(sb.toString(), strtPos);
			flag = true;
			if (indx < 0) {
				return stmt;
			}
		}
		int firstE=upStmt.indexOf("'");
		int endE=upStmt.lastIndexOf("'");

		StringBuilder sb = new StringBuilder();
		while (indx > 0) {
			sb.append(stmt.substring(strtPos, indx));
			strtPos = indx + upSchema.length();
			if (flag) {
				strtPos += 2;
			}
			if(indx>firstE&&indx<endE&&countChar(stmt,indx)%2==1)
			{
				sb.append(stmt.substring( indx,indx+schema.length()+1))  ;
			}
			indx = upStmt.indexOf(upSchema, strtPos);
		}
		sb.append(stmt.substring(strtPos));
		return sb.toString();
	}

	private static int countChar(String sql,int end)
	{
		int count=0;
		boolean skipChar = false;
		for (int i = 0; i < end; i++) {
			if(sql.charAt(i)=='\'' && !skipChar) {
				count++;
				skipChar = false;
			}else if( sql.charAt(i)=='\\'){
				skipChar = true;
			}else{
				skipChar = false;
			}
		}
		return count;
	}

	/**
	 * 获取第一个节点作为路由
	 *
	 * @param rrs		          数据路由集合
	 * @param dataNode  	数据库所在节点
	 * @param stmt   		执行语句
	 * @return 				数据路由集合
	 * 
	 * @author mycat
	 */
	public static RouteResultset routeToSingleNode(RouteResultset rrs,
			String dataNode, String stmt) {
		if (dataNode == null) {
			return rrs;
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);//rrs.getStatement()
		nodes[0].setSource(rrs);
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
		if (rrs.getCanRunInReadDB() != null) {
			nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
		}
		if(rrs.getRunOnSlave() != null){
			nodes[0].setRunOnSlave(rrs.getRunOnSlave());
		}
		
		return rrs;
	}


	public static RouteResultset routeToDDLNode(SchemaInfo schemaInfo, RouteResultset rrs, String stmt){
		stmt = getFixedSql(removeSchema(stmt,schemaInfo.schema));
		List<String> dataNodes = new ArrayList<>();
		Map<String, TableConfig> tables = schemaInfo.schemaConfig.getTables();
		TableConfig tc = tables.get(schemaInfo.table);
		if (tables != null && (tc != null)) {
			dataNodes = tc.getDataNodes();
		}
		Iterator<String> iterator1 = dataNodes.iterator();
		int nodeSize = dataNodes.size();
		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];

		for (int i = 0; i < nodeSize; i++) {
			String name = iterator1.next();
			nodes[i] = new RouteResultsetNode(name, ServerParse.DDL, stmt);
			nodes[i].setSource(rrs);
		}
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
		MycatServer.getInstance().getTmManager().addMetaLock(schemaInfo.schema, schemaInfo.table);
		return rrs;
	}

	

	/**
	 * 处理SQL
	 *
	 * @param stmt   执行语句
	 * @return 		 处理后SQL
	 * @author AStoneGod
	 */
	public static String getFixedSql(String stmt){
		stmt = stmt.replaceAll("\r\n", " "); //对于\r\n的字符 用 空格处理 rainbow
		return stmt = stmt.trim(); //.toUpperCase();    
	}

	/**
	 * 获取table名字
	 *
	 * @param stmt  	执行语句
	 * @param repPos	开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}
		int thiInd = stmt.indexOf('(',secInd+1);
		if (thiInd < 0) {
			thiInd = stmt.length();
		}
		repPos[1] = secInd;
		String tableName = "";
		if (stmt.toUpperCase().startsWith("DESC")||stmt.toUpperCase().startsWith("DESCRIBE")){
			tableName = stmt.substring(startPos, thiInd).trim();
		}else {
			tableName = stmt.substring(secInd, thiInd).trim();
		}

		//ALTER TABLE
		if (tableName.contains(" ")){
			tableName = tableName.substring(0,tableName.indexOf(" "));
		}
		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}


	/**
	 * 获取show语句table名字
	 *
	 * @param stmt	        执行语句
	 * @param repPos   开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getShowTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}

		repPos[1] = secInd;
		String tableName = stmt.substring(startPos, secInd).trim();

		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt     执行语句
	 * @param start      开始位置
	 * @return int[]	  关键字位置和占位个数
	 * 
	 * @author mycat
	 */
	public static int[] getCreateTablePos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getCreateIndexPos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取ALTER语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getAlterTablePos(String upStmt, int start) {
		String token1 = "ALTER ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt 	执行语句
	 * @param start  	开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getDropTablePos(String upStmt, int start) {
		//增加 if exists判断
		if(upStmt.contains("EXISTS")){
			String token1 = "IF ";
			String token2 = " EXISTS ";
			int ifInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);
			if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}else {
			String token1 = "DROP ";
			String token2 = " TABLE ";
			int createInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);

			if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}
	}


	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author aStoneGod
	 */

	public static int[] getDropIndexPos(String upStmt, int start) {
		String token1 = "DROP ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt    执行语句
	 * @param start     开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getTruncateTablePos(String upStmt, int start) {
		String token1 = "TRUNCATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
	 * @author mycat
	 */
	public static int[] getSpecPos(String upStmt, int start) {
		String token1 = " FROM ";
		String token2 = " IN ";
		int tabInd1 = upStmt.indexOf(token1, start);
		int tabInd2 = upStmt.indexOf(token2, start);
		if (tabInd1 > 0) {
			if (tabInd2 < 0) {
				return new int[] { tabInd1, token1.length() };
			}
			return (tabInd1 < tabInd2) ? new int[] { tabInd1, token1.length() }
					: new int[] { tabInd2, token2.length() };
		} else {
			return new int[] { tabInd2, token2.length() };
		}
	}

	/**
	 * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
	 *
	 * @param upStmt   执行sql
	 * @param start    开始位置
	 * @return int
	 * @author mycat
	 */
	public static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.toUpperCase().indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.toUpperCase().indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
	}

	public static boolean processWithMycatSeq(SchemaConfig schema, int sqlType,
	                                          String origSQL, ServerConnection sc) {
		// check if origSQL is with global sequence
		// @micmiu it is just a simple judgement
		//对应本地文件配置方式：insert into table1(id,name) values(next value for MYCATSEQ_GLOBAL,‘test’);
		if (origSQL.indexOf(" MYCATSEQ_") != -1) {
			processSQL(sc,schema,origSQL,sqlType);
			return true;
		}
		return false;
	}

	public static void processSQL(ServerConnection sc,SchemaConfig schema,String sql,int sqlType){
//		int sequenceHandlerType = MycatServer.getInstance().getConfig().getSystem().getSequnceHandlerType();
		SessionSQLPair sessionSQLPair = new SessionSQLPair(sc.getSession2(), schema, sql, sqlType);
//		if(sequenceHandlerType == 3 || sequenceHandlerType == 4){
//			DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(MycatServer
//					.getInstance().getConfig().getSystem().getSequnceHandlerType());
//			String charset = sessionSQLPair.session.getSource().getCharset();
//			String executeSql = null;
//			try {
//				executeSql = sequenceHandler.getExecuteSql(sessionSQLPair.sql,charset == null ? "utf-8":charset);
//			} catch (UnsupportedEncodingException e) {
//				LOGGER.error("UnsupportedEncodingException!");
//			}
//			sessionSQLPair.session.getSource().routeEndExecuteSQL(executeSql, sessionSQLPair.type,sessionSQLPair.schema);
//		} else {
			MycatServer.getInstance().getSequnceProcessor().addNewSql(sessionSQLPair);
//		}
	}

	public static boolean processInsert(SchemaConfig schema, int sqlType,
	                                    String origSQL, ServerConnection sc) throws SQLNonTransientException {
		String tableName = StringUtil.getTableName(origSQL);
		if (schema.getLowerCase() == 1) {
			tableName = tableName.toUpperCase();
		}
		TableConfig tableConfig = schema.getTables().get(tableName);
		boolean processedInsert=false;
		//判断是有自增字段
		if (null != tableConfig && tableConfig.isAutoIncrement()) {
			String primaryKey = tableConfig.getPrimaryKey();
			processedInsert=processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey);
		}
		return processedInsert;
	}

	private static boolean isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){
		
		if (primaryKey == null) {
			throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml");
		}
		
		boolean isPrimaryKeyInFields = false;
		String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1).toUpperCase();
		for (int pkOffset = 0, primaryKeyLength = primaryKey.length(), pkStart = 0;;) {
			pkStart = upperSQL.indexOf(primaryKey, pkOffset);
			if (pkStart >= 0 && pkStart < firstRightBracketIndex) {
				char pkSide = upperSQL.charAt(pkStart - 1);
				if (pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == '(') {
					pkSide = upperSQL.charAt(pkStart + primaryKey.length());
					isPrimaryKeyInFields = pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == ')';
				}
				if (isPrimaryKeyInFields) {
					break;
				}
				pkOffset = pkStart + primaryKeyLength;
			} else {
				break;
			}
		}
		return isPrimaryKeyInFields;
	}

	/*tableName has changed to UpperCase if LowerCase==1 */
	public static boolean processInsert(ServerConnection sc,SchemaConfig schema,
			int sqlType,String origSQL,String tableName,String primaryKey) throws SQLNonTransientException {

		int firstLeftBracketIndex = origSQL.indexOf("(");
		int firstRightBracketIndex = origSQL.indexOf(")");
		String upperSql = origSQL.toUpperCase();
		int valuesIndex = upperSql.indexOf("VALUES");
		int selectIndex = upperSql.indexOf("SELECT");
		int fromIndex = upperSql.indexOf("FROM");
		//屏蔽insert into table1 select * from table2语句
		if(firstLeftBracketIndex < 0) {
			String msg = "invalid sql:" + origSQL;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//屏蔽批量插入
		if(selectIndex > 0 &&fromIndex>0&&selectIndex>firstRightBracketIndex&&valuesIndex<0) {
			String msg = "multi insert not provided" ;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//插入语句必须提供列结构，因为MyCat默认对于表结构无感知
		if(valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
			throw new SQLSyntaxErrorException("insert must provide ColumnList");
		}
		//如果主键不在插入语句的fields中，则需要进一步处理
		boolean processedInsert=!isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
		if(processedInsert){
			List<String> insertSQLs = handleBatchInsert(origSQL, valuesIndex);
			for(String insertSQL:insertSQLs) {
				processInsert(sc, schema, sqlType, insertSQL, tableName, primaryKey, firstLeftBracketIndex + 1, insertSQL.indexOf('(', firstRightBracketIndex) + 1);
			}
		}
		return processedInsert;
	}

	public static List<String> handleBatchInsert(String origSQL, int valuesIndex){
		List<String> handledSQLs = new LinkedList<>();
		String prefix = origSQL.substring(0,valuesIndex + "VALUES".length());
		String values = origSQL.substring(valuesIndex + "VALUES".length());
		int flag = 0;
		StringBuilder currentValue = new StringBuilder();
		currentValue.append(prefix);
		for (int i = 0; i < values.length(); i++) {
			char j = values.charAt(i);
			if(j=='(' && flag == 0){
				flag = 1;
				currentValue.append(j);
			}else if(j=='\"' && flag == 1){
				flag = 2;
				currentValue.append(j);
			} else if(j=='\'' && flag == 1){
				flag = 2;
				currentValue.append(j);
			} else if(j=='\\' && flag == 2){
				flag = 3;
				currentValue.append(j);
			} else if (flag == 3){
				flag = 2;
				currentValue.append(j);
			}else if(j=='\"' && flag == 2){
				flag = 1;
				currentValue.append(j);
			} else if(j=='\'' && flag == 2){
				flag = 1;
				currentValue.append(j);
			} else if (j==')' && flag == 1){
				flag = 0;
				currentValue.append(j);
				handledSQLs.add(currentValue.toString());
				currentValue = new StringBuilder();
				currentValue.append(prefix);
			} else if(j == ',' && flag == 0){
				continue;
			} else {
				currentValue.append(j);
			}
		}
		return handledSQLs;
	}


	private static void processInsert(ServerConnection sc, SchemaConfig schema, int sqlType, String origSQL,
			String tableName, String primaryKey, int afterFirstLeftBracketIndex, int afterLastLeftBracketIndex) {
		/**
		 * 对于主键不在插入语句的fields中的SQL，需要改写。比如hotnews主键为id，插入语句为：
		 * insert into hotnews(title) values('aaa');
		 * 需要改写成：
		 * insert into hotnews(id, title) values(next value for MYCATSEQ_hotnews,'aaa');
 		 */
		int primaryKeyLength = primaryKey.length();
		int insertSegOffset = afterFirstLeftBracketIndex;
		String mycatSeqPrefix = "next value for MYCATSEQ_";
		int mycatSeqPrefixLength = mycatSeqPrefix.length();
		int tableNameLength = tableName.length();

		char[] newSQLBuf = new char[origSQL.length() + primaryKeyLength + mycatSeqPrefixLength + tableNameLength + 2];
		origSQL.getChars(0, afterFirstLeftBracketIndex, newSQLBuf, 0);
		primaryKey.getChars(0, primaryKeyLength, newSQLBuf, insertSegOffset);
		insertSegOffset += primaryKeyLength;
		newSQLBuf[insertSegOffset] = ',';
		insertSegOffset++;
		origSQL.getChars(afterFirstLeftBracketIndex, afterLastLeftBracketIndex, newSQLBuf, insertSegOffset);
		insertSegOffset += afterLastLeftBracketIndex - afterFirstLeftBracketIndex;
		mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
		insertSegOffset += mycatSeqPrefixLength;
		tableName.getChars(0, tableNameLength, newSQLBuf, insertSegOffset);
		insertSegOffset += tableNameLength;
		newSQLBuf[insertSegOffset] = ',';
		insertSegOffset++;
		origSQL.getChars(afterLastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
		processSQL(sc, schema, new String(newSQLBuf), sqlType);
	}

	public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt) {
		RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
		int i = 0;
		RouteResultsetNode node;
		for (String dataNode : dataNodes) {
			node = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
			node.setSource(rrs);
			if (rrs.getCanRunInReadDB() != null) {
				node.setCanRunInReadDB(rrs.getCanRunInReadDB());
			}
			if(rrs.getRunOnSlave() != null){
				nodes[0].setRunOnSlave(rrs.getRunOnSlave());
			}
			nodes[i++] = node;
		}
		rrs.setCacheAble(cache);
		rrs.setNodes(nodes);
		return rrs;
	}

	public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes,
			String stmt, boolean isGlobalTable) {
		
		rrs = routeToMultiNode(cache, rrs, dataNodes, stmt);
		rrs.setGlobalTable(isGlobalTable);
		return rrs;
	}

	public static void routeForTableMeta(RouteResultset rrs,
			SchemaConfig schema, String tableName, String sql) {
		String dataNode = null;
		if (isNoSharding(schema,tableName)) {//不分库的直接从schema中获取dataNode
			dataNode = schema.getDataNode();
		} else {
			dataNode = getMetaReadDataNode(schema, tableName);
		}

		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
		nodes[0].setSource(rrs);
		if (rrs.getCanRunInReadDB() != null) {
			nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
		}
		if(rrs.getRunOnSlave() != null){
			nodes[0].setRunOnSlave(rrs.getRunOnSlave());
		}
		rrs.setNodes(nodes);
	}

	/**
	 * 根据标名随机获取一个节点
	 *
	 * @param schema     数据库名
	 * @param table      表名
	 * @return 			  数据节点
	 * @author mycat
	 */
	private static String getMetaReadDataNode(SchemaConfig schema,
			String table) {
		String dataNode = null;
		if (schema.getLowerCase() == 1) {
			table = table.toUpperCase();
		}
		Map<String, TableConfig> tables = schema.getTables();
		TableConfig tc;
		if (tables != null && (tc = tables.get(table)) != null) {
			dataNode = getRandomDataNode(tc);
		}
		return dataNode;
	}

    private static String getRandomDataNode(TableConfig tc) {
        //写节点不可用，意味着读节点也不可用。
        //直接使用下一个 dataHost
        String randomDn = tc.getRandomDataNode();
        MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        if (mycatConfig != null) {
            PhysicalDBNode physicalDBNode = mycatConfig.getDataNodes().get(randomDn);
            if (physicalDBNode != null) {
                if (physicalDBNode.getDbPool().getSource().isAlive()) {
                    for (PhysicalDBPool pool : MycatServer.getInstance()
                            .getConfig()
                            .getDataHosts()
                            .values()) {
                        if (pool.getSource().getHostConfig().containDataNode(randomDn)) {
                            continue;
                        }

                        if (pool.getSource().isAlive()) {
                            return pool.getSource().getHostConfig().getRandomDataNode();
                        }
                    }
                }
            }
        }

        //all fail return default
        return randomDn;
    }

	public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
			Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {
		Set<String> retNodeSet = new LinkedHashSet<String>();
		// using parent rule to find datanode
		if (tc.isSecondLevel() && tc.getParentTC().getPartitionColumn().equals(tc.getParentKey())) {
			Set<String> nodeSet = ruleCalculate(tc.getParentTC(), colRoutePairSet);
			if (nodeSet.isEmpty()) {
				throw new SQLNonTransientException("parent key can't find  valid datanode ,expect 1 but found: " + nodeSet.size());
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  " + nodeSet + " sql :" + rrs.getStatement());
			}
			retNodeSet.addAll(nodeSet);
			return retNodeSet;
		} else {
			retNodeSet.addAll(tc.getParentTC().getDataNodes());
		}
		return retNodeSet;
	}

	public static Set<String> ruleCalculate(TableConfig tc, Set<ColumnRoutePair> colRoutePairSet)  {
		Set<String> routeNodeSet = new LinkedHashSet<String>();
		String col = tc.getRule().getColumn();
		RuleConfig rule = tc.getRule();
		AbstractPartitionAlgorithm algorithm = rule.getRuleAlgorithm();
		for (ColumnRoutePair colPair : colRoutePairSet) {
			if (colPair.colValue != null) {
				Integer nodeIndx = algorithm.calculate(colPair.colValue);
				if (nodeIndx == null) {
					throw new IllegalArgumentException("can't find datanode for sharding column:" + col + " val:" + colPair.colValue);
				} else {
					String dataNode = tc.getDataNodes().get(nodeIndx);
					routeNodeSet.add(dataNode);
					colPair.setNodeId(nodeIndx);
				}
			} else if (colPair.rangeValue != null) {
				Integer[] nodeRange = algorithm.calculateRange(String.valueOf(colPair.rangeValue.beginValue), String.valueOf(colPair.rangeValue.endValue));
				if (nodeRange != null) {
					/**
					 * 不能确认 colPair的 nodeid是否会有其它影响
					 */
					if (nodeRange.length == 0) {
						routeNodeSet.addAll(tc.getDataNodes());
					} else {
						ArrayList<String> dataNodes = tc.getDataNodes();
						String dataNode = null;
						for (Integer nodeId : nodeRange) {
							dataNode = dataNodes.get(nodeId);
							routeNodeSet.add(dataNode);
						}
					}
				}
			}

		}
		return routeNodeSet;
	}

	/**
	 * 多表路由
	 */
	public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
			RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool)
			throws SQLNonTransientException {
		
		List<String> tables = ctx.getTables();
		
		if(schema.isNoSharding()||(tables.size() >= 1&&isNoSharding(schema,tables.get(0)))) {
			return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
		}

		//只有一个表的
		if(tables.size() == 1) {
			return RouterUtil.tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect, cachePool);
		}

		Set<String> retNodesSet = new HashSet<String>();
		//每个表对应的路由映射
		Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();

		//分库解析信息不为空
		Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
		if(tablesAndConditions != null && tablesAndConditions.size() > 0) {
			//为分库表找路由
			RouterUtil.findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), cachePool, isSelect);
			if(rrs.isFinishedRoute()) {
				return rrs;
			}
		}

		//为全局表和单库表找路由
		for(String tableName : tables) {
			if (schema.getLowerCase() == 1) {
				tableName = tableName.toUpperCase();
			}
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig == null) {
				String msg = "can't find table define in schema "+ tableName + " schema:" + schema.getName();
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
			if(tableConfig.isGlobalTable()) {//全局表
				if(tablesRouteMap.get(tableName) == null) {
					tablesRouteMap.put(tableName, new HashSet<String>());
				}
				tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
			} else if(tablesRouteMap.get(tableName) == null) { //余下的表都是单库表
				tablesRouteMap.put(tableName, new HashSet<String>());
				tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
			}
		}

		boolean isFirstAdd = true;
		for(Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
			if(entry.getValue() == null || entry.getValue().size() == 0) {
				throw new SQLNonTransientException("parent key can't find any valid datanode ");
			} else {
				if(isFirstAdd) {
					retNodesSet.addAll(entry.getValue());
					isFirstAdd = false;
				} else {
					retNodesSet.retainAll(entry.getValue());
					if(retNodesSet.size() == 0) {//两个表的路由无交集
						String errMsg = "invalid route in sql, multi tables found but datanode has no intersection "
								+ " sql:" + ctx.getSql();
						LOGGER.warn(errMsg);
						throw new SQLNonTransientException(errMsg);
					}
				}
			}
		}

		if(retNodesSet != null && retNodesSet.size() > 0) {
			String tableName = tables.get(0);
			if (schema.getLowerCase() == 1) {
				tableName = tableName.toUpperCase();
			}
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig.isDistTable()){
				routeToDistTableNode(tableName,schema, rrs, ctx.getSql(), tablesAndConditions, cachePool, isSelect);
				return rrs;
			}
			
			if(retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
				// mulit routes ,not cache route result
				if (isSelect) {
					rrs.setCacheAble(false);
					routeToSingleNode(rrs, retNodesSet.iterator().next(), ctx.getSql());
				}
				else {//delete 删除全局表的记录
					routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(),true);
				}

			} else {
				routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
			}

		}
		return rrs;

	}


	/**
	 *
	 * 单表路由
	 */
	public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx,
			RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect,
			LayerCachePool cachePool) throws SQLNonTransientException {
		
		if (isNoSharding(schema, tableName)) {
			return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
		}
		if (schema.getLowerCase() == 1) {
			tableName = tableName.toUpperCase();
		}
		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table [" + tableName + "] define in schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		
		if(tc.isDistTable()){
			return routeToDistTableNode(tableName,schema,rrs,ctx.getSql(), routeUnit.getTablesAndConditions(), cachePool,isSelect);
		}
		
		if(tc.isGlobalTable()) {//全局表
			if(isSelect) {
				// global select ,not cache route result
				rrs.setCacheAble(false);
				return routeToSingleNode(rrs, getRandomDataNode(tc), ctx.getSql());
			} else {//insert into 全局表的记录
				return routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql(),true);
			}
		} else {//单表或者分库表
			if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
				throw new IllegalArgumentException("route rule for table "
						+ tc.getName() + " is required: " + ctx.getSql());

			}
			if(tc.getPartitionColumn() == null && !tc.isSecondLevel()) {//单表且不是childTable
//				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
				return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
			} else {
				//每个表对应的路由映射
				Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
				if(routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
					RouterUtil.findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, ctx.getSql(), cachePool, isSelect);
					if(rrs.isFinishedRoute()) {
						return rrs;
					}
				}

				if(tablesRouteMap.get(tableName) == null) {
					return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
				} else {
					return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName), ctx.getSql());
				}
			}
		}
	}
	/*tableName has changed to UpperCase if LowerCase==1 */
	private static RouteResultset routeToDistTableNode(String tableName, SchemaConfig schema, RouteResultset rrs,
			String orgSql, Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
			LayerCachePool cachePool, boolean isSelect) throws SQLNonTransientException {
		
		TableConfig tableConfig = schema.getTables().get(tableName);
		if(tableConfig == null) {
			String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		if(tableConfig.isGlobalTable()){
			String msg = "can't suport district table  " + tableName + " schema:" + schema.getName() + " for global table ";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		} 
		String partionCol = tableConfig.getPartitionColumn();
//		String primaryKey = tableConfig.getPrimaryKey();
        boolean isLoadData=false;
        
        Set<String> tablesRouteSet = new HashSet<String>();
        
        List<String> dataNodes = tableConfig.getDataNodes();
        if(dataNodes.size()>1){
			String msg = "can't suport district table  " + tableName + " schema:" + schema.getName() + " for mutiple dataNode " + dataNodes;
        	LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
        }
        String dataNode = dataNodes.get(0);
        
		//主键查找缓存暂时不实现
        if(tablesAndConditions.isEmpty()){
        	List<String> subTables = tableConfig.getDistTables();
        	tablesRouteSet.addAll(subTables);
        }
        
		for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
			boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
			Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
			
			Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
			if(partitionValue == null || partitionValue.size() == 0) {
				tablesRouteSet.addAll(tableConfig.getDistTables());
			} else {
				for(ColumnRoutePair pair : partitionValue) {
					AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
					if(pair.colValue != null) {
						Integer tableIndex = algorithm.calculate(pair.colValue);
						if(tableIndex == null) {
							String msg = "can't find any valid datanode :" + tableConfig.getName()
									+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
							LOGGER.warn(msg);
							throw new SQLNonTransientException(msg);
						}
						String subTable = tableConfig.getDistTables().get(tableIndex);
						if(subTable != null) {
							tablesRouteSet.add(subTable);
						}
					}
					if(pair.rangeValue != null) {
						Integer[] tableIndexs = algorithm
								.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
						for(Integer idx : tableIndexs) {
							String subTable = tableConfig.getDistTables().get(idx);
							if(subTable != null) {
								tablesRouteSet.add(subTable);
							}
						}
					}
				}
			}
		}

		Object[] subTables =  tablesRouteSet.toArray();
		RouteResultsetNode[] nodes = new RouteResultsetNode[subTables.length];
		for(int i=0;i<nodes.length;i++){
			String table = String.valueOf(subTables[i]);
			String changeSql = orgSql;
			nodes[i] = new RouteResultsetNode(dataNode, rrs.getSqlType(), changeSql);//rrs.getStatement()
			nodes[i].setSubTableName(table);
			nodes[i].setSource(rrs);
			if (rrs.getCanRunInReadDB() != null) {
				nodes[i].setCanRunInReadDB(rrs.getCanRunInReadDB());
			}
			if(rrs.getRunOnSlave() != null){
				nodes[0].setRunOnSlave(rrs.getRunOnSlave());
			}
		}
		rrs.setNodes(nodes);
		rrs.setSubTables(tablesRouteSet);
		rrs.setFinishedRoute(true);
		
		return rrs;
	}

	/**
	 * 处理分库表路由
	 */
	public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
			Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
			Map<String, Set<String>> tablesRouteMap, String sql, LayerCachePool cachePool, boolean isSelect)
			throws SQLNonTransientException {
		
		//为分库表找路由
		for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
			String tableName = entry.getKey();
			if (schema.getLowerCase() == 1) {
				tableName = tableName.toUpperCase();
			}
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig == null) {
				String msg = "can't find table define in schema "
						+ tableName + " schema:" + schema.getName();
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
			if(tableConfig.getDistTables()!=null && tableConfig.getDistTables().size()>0){
				routeToDistTableNode(tableName,schema,rrs,sql, tablesAndConditions, cachePool,isSelect);
			}
			//全局表或者不分库的表略过（全局表后面再计算）
			if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
				continue;
			} else {//非全局表：分库表、childTable、其他
				Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
				String joinKey = tableConfig.getJoinKey();
				String partionCol = tableConfig.getPartitionColumn();
				String primaryKey = tableConfig.getPrimaryKey();
				boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
                boolean isLoadData=false;
                if (LOGGER.isDebugEnabled()
						&& sql.startsWith(LoadData.loadDataHint)||rrs.isLoadData()) {
                     //由于load data一次会计算很多路由数据，如果输出此日志会极大降低load data的性能
                         isLoadData=true;
                }
				if(entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1&&!isLoadData)
                {//主键查找
					// try by primary key if found in cache
					Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
					if (primaryKeyPairs != null) {
						if (LOGGER.isDebugEnabled()) {
                                 LOGGER.debug("try to find cache by primary key ");
						}
						String tableKey = schema.getName() + '_' + tableName;
						boolean allFound = true;
						for (ColumnRoutePair pair : primaryKeyPairs) {//可能id in(1,2,3)多主键
							String cacheKey = pair.colValue;
							String dataNode = (String) cachePool.get(tableKey, cacheKey);
							if (dataNode == null) {
								allFound = false;
								continue;
							} else {
								if(tablesRouteMap.get(tableName) == null) {
									tablesRouteMap.put(tableName, new HashSet<String>());
								}
								tablesRouteMap.get(tableName).add(dataNode);
								continue;
							}
						}
						if (!allFound) {
							// need cache primary key ->datanode relation
							if (isSelect && tableConfig.getPrimaryKey() != null) {
								rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
							}
						} else {//主键缓存中找到了就执行循环的下一轮
							continue;
						}
					}
				}
				if (isFoundPartitionValue) {//分库表
					Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
					if(partitionValue == null || partitionValue.size() == 0) {
						if(tablesRouteMap.get(tableName) == null) {
							tablesRouteMap.put(tableName, new HashSet<String>());
						}
						tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
					} else {
						for(ColumnRoutePair pair : partitionValue) {
							AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
							if(pair.colValue != null) {
								Integer nodeIndex = algorithm.calculate(pair.colValue);
								if(nodeIndex == null) {
									String msg = "can't find any valid datanode :" + tableConfig.getName()
											+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
									LOGGER.warn(msg);
									throw new SQLNonTransientException(msg);
								}

								ArrayList<String> dataNodes = tableConfig.getDataNodes();
								String node;
								if (nodeIndex >=0 && nodeIndex < dataNodes.size()) {
									node = dataNodes.get(nodeIndex);

								} else {
									node = null;
									String msg = "Can't find a valid data node for specified node index :"
											+ tableConfig.getName() + " -> " + tableConfig.getPartitionColumn()
											+ " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
									LOGGER.warn(msg);
									throw new SQLNonTransientException(msg);
								}
								if(node != null) {
									if(tablesRouteMap.get(tableName) == null) {
										tablesRouteMap.put(tableName, new HashSet<String>());
									}
									tablesRouteMap.get(tableName).add(node);
								}
							}
							if(pair.rangeValue != null) {
								Integer[] nodeIndexs = algorithm
										.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
								ArrayList<String> dataNodes = tableConfig.getDataNodes();
								String node;
								for(Integer idx : nodeIndexs) {
									if (idx >= 0 && idx < dataNodes.size()) {
										node = dataNodes.get(idx);
									} else {
										String msg = "Can't find valid data node(s) for some of specified node indexes :"
												+ tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
										LOGGER.warn(msg);
										throw new SQLNonTransientException(msg);
									}
									if(node != null) {
										if(tablesRouteMap.get(tableName) == null) {
											tablesRouteMap.put(tableName, new HashSet<String>());
										}
										tablesRouteMap.get(tableName).add(node);

									}
								}
							}
						}
					}
				} else if(joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {//childTable  (如果是select 语句的父子表join)之前要找到root table,将childTable移除,只留下root table
					Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);
					
					Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

					if (dataNodeSet.isEmpty()) {
						throw new SQLNonTransientException(
								"parent key can't find any valid datanode ");
					}
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
								+ Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
					}
					if (dataNodeSet.size() > 1) {
						routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet, sql);
						rrs.setFinishedRoute(true);
						return;
					} else {
						rrs.setCacheAble(true);
						routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
						return;
					}

				} else {
					//没找到拆分字段，该表的所有节点都路由
					if(tablesRouteMap.get(tableName) == null) {
						tablesRouteMap.put(tableName, new HashSet<String>());
					}
					tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
				}
			}
		}
	}

	public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
		boolean isAllGlobal = false;
		for(String table : ctx.getTables()) {
			TableConfig tableConfig = schema.getTables().get(table);
			if(tableConfig!=null && tableConfig.isGlobalTable()) {
				isAllGlobal = true;
			} else {
				return false;
			}
		}
		return isAllGlobal;
	}

	/**
	 *
	 * @param schema
	 * @param ctx
	 * @param tc
	 * @return true表示校验通过，false表示检验不通过
	 */
	public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, TableConfig tc) {
		if(!tc.isRuleRequired()) {
			return true;
		}
		boolean hasRequiredValue = false;
		String tableName = tc.getName();
		if(routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
			hasRequiredValue = false;
		} else {
			for(Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

				String colName = condition.getKey();
				//条件字段是拆分字段
				if(colName.equals(tc.getPartitionColumn())) {
					hasRequiredValue = true;
					break;
				}
			}
		}
		return hasRequiredValue;
	}


	/**
	 * 增加判断支持未配置分片的表走默认的dataNode
	 * @param schemaConfig
	 * @param tableName
	 * @return
	 */
	public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
		if (schemaConfig.isNoSharding()) {
			return true;
		}
		if (schemaConfig.getLowerCase() == 1) {
			tableName = tableName.toUpperCase();
		}
		if (schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName)) {
			return true;
		}

		return false;
	}

	/**
	 * 判断条件是否永真
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysTrue(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
		if(Boolean.TRUE.equals(o)) {
			return true;
		}
		return false;
	}

	/**
	 * 判断条件是否永假的
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysFalse(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
		if(Boolean.FALSE.equals(o)) {
			return true;
		}
		return false;
	}
}
