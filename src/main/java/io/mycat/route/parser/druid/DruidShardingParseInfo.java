package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * druid parser result
 * @author wang.dw
 *
 */
public class DruidShardingParseInfo {
	
	private List<RouteCalculateUnit> routeCalculateUnits = new ArrayList<RouteCalculateUnit>();
	
	//tables为路由计算共享属性，多组RouteCalculateUnit使用同样的tables
	private List<String> tables = new ArrayList<String>();

	/**
	 * key table alias, value talbe realname;
	 */
	private Map<String, String> tableAliasMap = new LinkedHashMap<String, String>();

	public Map<String, String> getTableAliasMap() {
		return tableAliasMap;
	}

	public void setTableAliasMap(Map<String, String> tableAliasMap) {
		this.tableAliasMap = tableAliasMap;
	}

	public List<String> getTables() {
		return tables;
	}

	public void addTable(String tableName) {
		this.tables.add(tableName);
	}

	public RouteCalculateUnit getRouteCalculateUnit() {
		return routeCalculateUnits.get(0);
	}
	
	public List<RouteCalculateUnit> getRouteCalculateUnits() {
		return routeCalculateUnits;
	}
	
	public void setRouteCalculateUnits(List<RouteCalculateUnit> routeCalculateUnits) {
		this.routeCalculateUnits = routeCalculateUnits;
	}
	
	public void addRouteCalculateUnit(RouteCalculateUnit routeCalculateUnit) {
		this.routeCalculateUnits.add(routeCalculateUnit);
	}
	

	public void clear() {
		for(RouteCalculateUnit unit : routeCalculateUnits ) {
			unit.clear();
		}
	}
	

}
