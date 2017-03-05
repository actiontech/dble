package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.stat.TableStat.Condition;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.sqlengine.mpp.RangeValue;
import io.mycat.util.StringUtil;

/**
 * 对SQLStatement解析
 * 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 *  有些只能通过statement解析才能得到所有信息
 *  有些需要通过两种方式解析才能得到完整信息
 * @author wang.dw
 *
 */
public class DefaultDruidParser implements DruidParser {
	protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);
	/**
	 * 解析得到的结果
	 */
	protected DruidShardingParseInfo ctx;
	
	/**
	 * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
	 * @param schema
	 * @param stmt
	 */
	public SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,LayerCachePool cachePool,MycatSchemaStatVisitor schemaStatVisitor) throws SQLNonTransientException {
		ctx = new DruidShardingParseInfo();
		//通过visitor解析
		schema = visitorParse(schema, rrs,stmt,schemaStatVisitor);
		
		//改写sql：如insert语句主键自增长的可以
		changeSql(schema, rrs, stmt, cachePool);
		return schema;
	}
	
	 
	
	/**
	 * 改写sql：如insert是
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {
		
	}

	/**
	 * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
	 * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
	 * @param stmt
	 */
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		stmt.accept(visitor);
		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if (visitor.hasOrCondition()) {
			// 根据or拆分
			mergedConditionList = visitor.splitConditions();
		} else {// 不包含OR语句
			mergedConditionList.add(visitor.getConditions());
		}
		Map<String,String> tableAliasMap = getTableAliasMap(visitor.getAliasMap());
		ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(tableAliasMap, mergedConditionList));
		return schema;
	}
	private Map<String,String> getTableAliasMap(Map<String,String> originTableAliasMap){
		if (originTableAliasMap == null) {
			return null;
		}
		Map<String,String> tableAliasMap = new HashMap<String,String>();
		tableAliasMap.putAll(originTableAliasMap);
		for (Map.Entry<String, String> entry : originTableAliasMap.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			if (MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
				key = key.toLowerCase();
				value = value.toLowerCase();
			}
			if (key != null && key.indexOf("`") >= 0) {
				key = key.replaceAll("`", "");
			}
			if (value != null && value.indexOf("`") >= 0) {
				value = value.replaceAll("`", "");
			}
			// 表名前面带database的，去掉
			if (key != null) {
				boolean needAddTable = false;
				if (key.equals(value)) {
					needAddTable = true;
				}
				int pos = key.indexOf(".");
				if (pos > 0) {
					key = key.substring(pos + 1);
				}
				if (needAddTable) {
					ctx.addTable(key);
				}
				tableAliasMap.put(key, value);
			}
		}
		ctx.setTableAliasMap(tableAliasMap);
		return tableAliasMap;
	}
	private List<RouteCalculateUnit> buildRouteCalculateUnits(Map<String,String> tableAliasMap, List<List<Condition>> conditionList) {
		List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();
		//遍历condition ，找分片字段
		for(int i = 0; i < conditionList.size(); i++) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			for(Condition condition : conditionList.get(i)) {
				List<Object> values = condition.getValues();
				if(values.size() == 0) {
					break;
				}
				if(checkConditionValues(values)) {
					String columnName = StringUtil.removeBackquote(condition.getColumn().getName().toUpperCase());
					String tableName = StringUtil.removeBackquote(condition.getColumn().getTable());
					if (MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
						tableName = tableName.toLowerCase();
					}
					if(tableAliasMap != null && tableAliasMap.get(tableName) == null) {//子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
						continue;
					}
					if(tableAliasMap != null && tableAliasMap.get(tableName) != null 
							&& !tableAliasMap.get(tableName).equals(tableName)) {
						tableName = tableAliasMap.get(tableName);
					}
					String operator = condition.getOperator();
					
					//只处理between ,in和=3中操作符
					if(operator.equals("between")) {
						RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
								routeCalculateUnit.addShardingExpr(tableName, columnName, rv);
					} else if(operator.equals("=") || operator.toLowerCase().equals("in")){ //只处理=号和in操作符,其他忽略
								routeCalculateUnit.addShardingExpr(tableName, columnName, values.toArray());
					}
				}
			}
			retList.add(routeCalculateUnit);
		}
		return retList;
	}
	
	private boolean checkConditionValues(List<Object> values) {
		for(Object value : values) {
			if(value != null && !value.toString().equals("")) {
				return true;
			}
		}
		return false;
	}
	
	public DruidShardingParseInfo getCtx() {
		return ctx;
	}
}
