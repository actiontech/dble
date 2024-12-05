/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid;

import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.alibaba.druid.stat.TableStat;
import com.google.common.collect.Sets;

import java.util.*;

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

    private Set<TableStat.Relationship> relationship = Sets.newHashSet();

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

    public Set<TableStat.Relationship> getRelationship() {
        return relationship;
    }

    public void setRelationship(Set<TableStat.Relationship> relationship) {
        this.relationship = relationship;
    }
}
