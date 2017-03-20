/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.config.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author mycat
 */
public class SchemaConfig {
	private final Random random = new Random();
	private final String name;
	private final Map<String, TableConfig> tables;
	private final boolean noSharding;
	private final String dataNode;
	private final Set<String> metaDataNodes;
	private final Set<String> allDataNodes;
	/**
	 * when a select sql has no limit condition ,and default max limit to
	 * prevent memory problem when return a large result set
	 */
	private final int defaultMaxLimit;
	private final String[] allDataNodeStrArr;
	private Map<ERTable, Set<ERTable>> FkErRelations;
	private Map<String, Set<ERTable>> funcNodeERMap;
	public SchemaConfig(String name, String dataNode,
			Map<String, TableConfig> tables, int defaultMaxLimit) {
		this.name = name;
		this.dataNode = dataNode;
		this.tables = tables;
		this.defaultMaxLimit = defaultMaxLimit;
		buildERMap(tables);
		this.noSharding = (tables == null || tables.isEmpty());
		if (noSharding && dataNode == null) {
			throw new RuntimeException(name + " in noSharding mode schema must have default dataNode ");
		}
		this.metaDataNodes = buildMetaDataNodes();
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
	private void buildERMap(Map<String, TableConfig> tables2) {
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
					funcNodeERMap = new HashMap<String, Set<ERTable>>();
				}
				Set<ERTable> eratables = funcNodeERMap.get(key);
				if (eratables == null) {
					eratables = new HashSet<ERTable>();
					funcNodeERMap.put(key, eratables);
				}
				eratables.add(new ERTable(name, tc.getName(), column));
				continue;
			}
			if (parent.getDirectRouteTC() == null || tc.getDirectRouteTC() == null) {
				if (FkErRelations == null) {
					FkErRelations = new HashMap<ERTable, Set<ERTable>>();
				}
				ERTable parentTable = new ERTable(name, parent.getName(), tc.getParentKey());
				ERTable childTable = new ERTable(name, tc.getName(), tc.getJoinKey());
				Set<ERTable> relationParent = FkErRelations.get(parentTable);
				if (relationParent == null) {
					relationParent = new HashSet<ERTable>(1);
				}
				relationParent.add(childTable);
				FkErRelations.put(parentTable, relationParent);

				Set<ERTable> relationChild = FkErRelations.get(childTable);
				if (relationChild == null) {
					relationChild = new HashSet<ERTable>(1);
				}
				relationChild.add(parentTable);
				FkErRelations.put(childTable, relationChild);
			} else {
				if (tc.getDirectRouteTC() != null) {
					TableConfig root = tc.getDirectRouteTC();
					String key = root.getRule().getRuleAlgorithm().getName() + "_" + root.getDataNodes().toString();
					if (funcNodeERMap == null) {
						funcNodeERMap = new HashMap<String, Set<ERTable>>();
					}
					Set<ERTable> eratables = funcNodeERMap.get(key);
					if (eratables == null) {
						eratables = new HashSet<ERTable>();
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

	public Set<String> getMetaDataNodes() {
		return metaDataNodes;
	}

	public Set<String> getAllDataNodes() {
		return allDataNodes;
	}

	public Map<ERTable, Set<ERTable>> getFkErRelations() {
		return FkErRelations;
	}
	public String getRandomDataNode() {
		if (this.allDataNodeStrArr == null) {
			return null;
		}
		int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allDataNodeStrArr.length;
		return this.allDataNodeStrArr[index];
	}

	/**
	 * 取得含有不同Meta信息的数据节点,比如表和表结构。
	 */
	private Set<String> buildMetaDataNodes() {
		Set<String> set = new HashSet<String>();
		if (!isEmpty(dataNode)) {
			set.add(dataNode);
		}
		if (!noSharding) {
			for (TableConfig tc : tables.values()) {
				set.add(tc.getDataNodes().get(0));
			}
		}

		return set;
	}

	/**
	 * 取得该schema的所有数据节点
	 */
	private Set<String> buildAllDataNodes() {
		Set<String> set = new HashSet<String>();
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