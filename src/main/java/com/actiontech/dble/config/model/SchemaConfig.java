/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import java.util.*;

/**
 * @author mycat
 */
public class SchemaConfig {
    private final Random random = new Random();
    private final String name;
    private final Map<String, TableConfig> tables;
    private final boolean noSharding;
    private final String dataNode;
    private final String metaDataNode;
    private final Set<String> allDataNodes;
    /**
     * when a select sql has no limit condition ,and default max limit to
     * prevent memory problem when return a large result set
     */
    private final int defaultMaxLimit;
    private final String[] allDataNodeStrArr;
    private Map<ERTable, Set<ERTable>> fkErRelations;
    private Map<String, Set<ERTable>> funcNodeERMap;

    public SchemaConfig(String name, String dataNode,
                        Map<String, TableConfig> tables, int defaultMaxLimit) {
        this.name = name;
        this.dataNode = dataNode;
        this.tables = tables;
        this.defaultMaxLimit = defaultMaxLimit;
        buildERMap();
        this.noSharding = (tables == null || tables.isEmpty());
        if (noSharding && dataNode == null) {
            throw new RuntimeException(name + " in noSharding mode schema must have default dataNode ");
        }
        this.metaDataNode = buildMetaDataNodes();
        this.allDataNodes = buildAllDataNodes();
        if (this.allDataNodes != null && !this.allDataNodes.isEmpty()) {
            String[] dnArr = new String[this.allDataNodes.size()];
            dnArr = this.allDataNodes.toArray(dnArr);
            this.allDataNodeStrArr = dnArr;
        } else {
            this.allDataNodeStrArr = null;
        }
    }

    public int getDefaultMaxLimit() {
        return defaultMaxLimit;
    }

    private void buildERMap() {
        if (tables == null || tables.isEmpty()) {
            return;
        }
        for (TableConfig tc : tables.values()) {
            TableConfig parent = tc.getParentTC();
            if (parent == null) {
                // noraml table may has the same funaction add date node with other tables
                TableConfig root = tc.getDirectRouteTC();
                if (tc.isGlobalTable() || tc.getRule() == null) {
                    continue;
                }
                String key = tc.getRule().getRuleAlgorithm().getName() + "_" + root.getDataNodes().toString();
                String column = root.getRule().getColumn();
                if (funcNodeERMap == null) {
                    funcNodeERMap = new HashMap<>();
                }
                Set<ERTable> eratables = funcNodeERMap.get(key);
                if (eratables == null) {
                    eratables = new HashSet<>();
                    funcNodeERMap.put(key, eratables);
                }
                eratables.add(new ERTable(name, tc.getName(), column));
                continue;
            }
            if (parent.getDirectRouteTC() == null || tc.getDirectRouteTC() == null) {
                if (fkErRelations == null) {
                    fkErRelations = new HashMap<>();
                }
                ERTable parentTable = new ERTable(name, parent.getName(), tc.getParentKey());
                ERTable childTable = new ERTable(name, tc.getName(), tc.getJoinKey());
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
            } else {
                if (tc.getDirectRouteTC() != null) {
                    TableConfig root = tc.getDirectRouteTC();
                    String key = root.getRule().getRuleAlgorithm().getName() + "_" + root.getDataNodes().toString();
                    if (funcNodeERMap == null) {
                        funcNodeERMap = new HashMap<>();
                    }
                    Set<ERTable> eratables = funcNodeERMap.get(key);
                    if (eratables == null) {
                        eratables = new HashSet<>();
                        funcNodeERMap.put(key, eratables);
                    }
                    eratables.add(new ERTable(name, tc.getName(), tc.getJoinKey()));
                    eratables.add(new ERTable(name, parent.getName(), tc.getParentKey()));
                }
            }
        }
    }

    public String getName() {
        return name;
    }

    public String getDataNode() {
        return dataNode;
    }

    public Map<String, TableConfig> getTables() {
        return tables;
    }

    public boolean isNoSharding() {
        return noSharding;
    }

    public String getMetaDataNode() {
        return metaDataNode;
    }

    public Set<String> getAllDataNodes() {
        return allDataNodes;
    }

    public Map<ERTable, Set<ERTable>> getFkErRelations() {
        return fkErRelations;
    }

    public String getRandomDataNode() {
        if (this.allDataNodeStrArr == null) {
            return null;
        }
        int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allDataNodeStrArr.length;
        return this.allDataNodeStrArr[index];
    }

    /**
     * schema's default dataNode,used for show tables
     */
    private String buildMetaDataNodes() {
        if (!isEmpty(dataNode)) {
            return dataNode;
        } else {
            for (TableConfig tc : tables.values()) {
                return tc.getDataNodes().get(0);
            }
            throw new RuntimeException(name + " in Sharding mode schema must have at least one table ");
        }
    }

    private Set<String> buildAllDataNodes() {
        Set<String> set = new HashSet<>();
        if (!isEmpty(dataNode)) {
            set.add(dataNode);
        }
        if (!noSharding) {
            for (TableConfig tc : tables.values()) {
                set.addAll(tc.getDataNodes());
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
