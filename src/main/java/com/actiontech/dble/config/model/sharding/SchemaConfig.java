/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.sharding;

import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;

import java.util.*;

/**
 * @author mycat
 */
public class SchemaConfig {
    private final Random random = new Random();
    private final String name;
    private final Map<String, BaseTableConfig> tables;
    private final boolean noSharding;
    private final String shardingNode;
    private final String metaShardingNode;
    private final Set<String> allShardingNodes;
    /**
     * when a select sql has no limit condition ,and default max limit to
     * prevent memory problem when return a large result set
     */
    private final int defaultMaxLimit;
    private final String[] allShardingNodeStrArr;
    private Map<ERTable, Set<ERTable>> fkErRelations;
    private Map<String, Set<ERTable>> funcNodeERMap;

    public SchemaConfig(String name, String shardingNode,
                        Map<String, BaseTableConfig> tables, int defaultMaxLimit) {
        this.name = name;
        this.shardingNode = shardingNode;
        this.tables = tables;
        this.defaultMaxLimit = defaultMaxLimit;
        buildERMap();
        this.noSharding = (tables == null || tables.isEmpty());
        if (noSharding && shardingNode == null) {
            throw new RuntimeException(name + " in noSharding mode schema must have default shardingNode ");
        }
        this.metaShardingNode = buildMetaShardingNodes();
        this.allShardingNodes = buildAllShardingNodes();
        if (this.allShardingNodes != null && !this.allShardingNodes.isEmpty()) {
            String[] dnArr = new String[this.allShardingNodes.size()];
            dnArr = this.allShardingNodes.toArray(dnArr);
            this.allShardingNodeStrArr = dnArr;
        } else {
            this.allShardingNodeStrArr = null;
        }
    }

    public SchemaConfig(SchemaConfig oldSchemaConfig) {
        this.name = oldSchemaConfig.getName().toLowerCase();
        this.shardingNode = oldSchemaConfig.getShardingNode();
        this.tables = oldSchemaConfig.getLowerCaseTables();
        this.defaultMaxLimit = oldSchemaConfig.getDefaultMaxLimit();
        buildERMap();
        this.noSharding = (tables == null || tables.isEmpty());
        if (noSharding && shardingNode == null) {
            throw new RuntimeException(name + " in noSharding mode schema must have default shardingNode ");
        }
        this.metaShardingNode = buildMetaShardingNodes();
        this.allShardingNodes = buildAllShardingNodes();
        if (this.allShardingNodes != null && !this.allShardingNodes.isEmpty()) {
            String[] dnArr = new String[this.allShardingNodes.size()];
            dnArr = this.allShardingNodes.toArray(dnArr);
            this.allShardingNodeStrArr = dnArr;
        } else {
            this.allShardingNodeStrArr = null;
        }
    }


    public int getDefaultMaxLimit() {
        return defaultMaxLimit;
    }

    private void buildERMap() {
        if (tables == null || tables.isEmpty()) {
            return;
        }
        for (BaseTableConfig tc : tables.values()) {
            if (tc instanceof ShardingTableConfig) {
                // normal table may has the same function add date node with other tables
                ShardingTableConfig shardingTable = (ShardingTableConfig) tc;
                String key = shardingTable.getFunction().getAlias() + "_" + shardingTable.getShardingNodes().toString();
                String column = shardingTable.getShardingColumn();
                if (funcNodeERMap == null) {
                    funcNodeERMap = new HashMap<>();
                }
                Set<ERTable> eraTables = funcNodeERMap.computeIfAbsent(key, k -> new HashSet<>());
                eraTables.add(new ERTable(name, tc.getName(), column));
            } else if (tc instanceof ChildTableConfig) {
                ChildTableConfig childTableConfig = (ChildTableConfig) tc;
                BaseTableConfig parent = childTableConfig.getParentTC();
                if (childTableConfig.getDirectRouteTC() != null) {
                    ShardingTableConfig root = childTableConfig.getDirectRouteTC();
                    String key = root.getFunction().getAlias() + "_" + root.getShardingNodes().toString();
                    if (funcNodeERMap == null) {
                        funcNodeERMap = new HashMap<>();
                    }
                    Set<ERTable> erTables = funcNodeERMap.computeIfAbsent(key, k -> new HashSet<>());
                    erTables.add(new ERTable(name, childTableConfig.getName(), childTableConfig.getJoinColumn()));
                    erTables.add(new ERTable(name, parent.getName(), childTableConfig.getParentColumn()));
                } else {
                    if (fkErRelations == null) {
                        fkErRelations = new HashMap<>();
                    }
                    ERTable parentTable = new ERTable(name, parent.getName(), childTableConfig.getParentColumn());
                    ERTable childTable = new ERTable(name, childTableConfig.getName(), childTableConfig.getJoinColumn());
                    Set<ERTable> relationParent = fkErRelations.get(parentTable);
                    if (relationParent == null) {
                        relationParent = new HashSet<>(1);
                    }
                    relationParent.add(childTable);
                    fkErRelations.put(parentTable, relationParent);

                    Set<ERTable> relationChild = fkErRelations.get(childTable);
                    if (relationChild == null) {
                        relationChild = new HashSet<>(1);
                    }
                    relationChild.add(parentTable);
                    fkErRelations.put(childTable, relationChild);
                }
            }
        }
    }

    public String getName() {
        return name;
    }

    public String getShardingNode() {
        return shardingNode;
    }

    public Map<String, BaseTableConfig> getTables() {
        return tables;
    }
    public BaseTableConfig getTable(String tableName) {
        return tables.get(tableName);
    }

    private Map<String, BaseTableConfig> getLowerCaseTables() {
        Map<String, BaseTableConfig> newTables = new HashMap<>();

        //first round is only get the top tables
        List<BaseTableConfig> valueList = new ArrayList<>(tables.values());
        Iterator<BaseTableConfig> it = valueList.iterator();
        while (it.hasNext()) {
            BaseTableConfig tc = it.next();
            if (!(tc instanceof ChildTableConfig)) {
                newTables.put(tc.getName().toLowerCase(), tc.lowerCaseCopy(null));
                it.remove();
            }
        }

        while (valueList.size() > 0) {
            Iterator<BaseTableConfig> its = valueList.iterator();
            while (its.hasNext()) {
                ChildTableConfig tc = (ChildTableConfig) (its.next());
                String parentName = tc.getParentTC().getName().toLowerCase();
                if (newTables.containsKey(parentName)) {
                    BaseTableConfig parent = newTables.get(parentName);
                    newTables.put(tc.getName().toLowerCase(), tc.lowerCaseCopy(parent));
                    its.remove();
                }
            }
        }

        return newTables;
    }

    public boolean isNoSharding() {
        return noSharding;
    }

    public String getMetaShardingNode() {
        return metaShardingNode;
    }

    public Set<String> getAllShardingNodes() {
        return allShardingNodes;
    }

    public Map<ERTable, Set<ERTable>> getFkErRelations() {
        return fkErRelations;
    }

    public String getRandomShardingNode() {
        if (this.allShardingNodeStrArr == null) {
            return null;
        }
        int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allShardingNodeStrArr.length;
        return this.allShardingNodeStrArr[index];
    }

    /**
     * sharding's default shardingNode,used for show tables
     */
    private String buildMetaShardingNodes() {
        if (!isEmpty(shardingNode)) {
            return shardingNode;
        } else {
            for (BaseTableConfig tc : tables.values()) {
                return tc.getShardingNodes().get(0);
            }
            throw new RuntimeException(name + " in Sharding mode schema must have at least one table ");
        }
    }

    private Set<String> buildAllShardingNodes() {
        Set<String> set = new HashSet<>();
        if (!isEmpty(shardingNode)) {
            set.add(shardingNode);
        }
        if (!noSharding) {
            for (BaseTableConfig tc : tables.values()) {
                set.addAll(tc.getShardingNodes());
            }
        }
        return set;
    }

    private static boolean isEmpty(String str) {
        return ((str == null) || (str.length() == 0));
    }

    public Map<String, Set<ERTable>> getFuncNodeERMap() {
        return funcNodeERMap;
    }

}
