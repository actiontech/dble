/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.route.parser.util.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * druid parser result
 *
 * @author wang.dw
 */
public class DruidShardingParseInfo {

    private List<RouteCalculateUnit> routeCalculateUnits = new ArrayList<>();

    private List<Pair<String, String>> tables = new ArrayList<>();

    /**
     * key table alias, value table real name;
     */
    private Map<String, String> tableAliasMap = new LinkedHashMap<>();

    public Map<String, String> getTableAliasMap() {
        return tableAliasMap;
    }

    public void setTableAliasMap(Map<String, String> tableAliasMap) {
        this.tableAliasMap = tableAliasMap;
    }

    public List<Pair<String, String>> getTables() {
        return tables;
    }

    public void addTable(Pair<String, String> table) {
        this.tables.add(table);
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

    public void clearRouteCalculateUnit() {
        for (RouteCalculateUnit unit : routeCalculateUnits) {
            unit.clear();
        }
        routeCalculateUnits.clear();
    }

}
