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
package io.mycat.route;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLStatement;

import io.mycat.config.model.SchemaConfig;
import io.mycat.server.NonBlockingSession;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.util.FormatUtil;

/**
 * @author mycat
 */
public final class RouteResultset implements Serializable {
	private static final long serialVersionUID = 3906972758236875720L;
	private final String srcStatement;// 原始语句
	private String statement;
	private String schema;
	private String table;
    private final int sqlType;
    private RouteResultsetNode[] nodes; // 路由结果节点
    private SQLStatement sqlStatement; 
    
    private boolean needOptimizer;
    private int limitStart;
    private boolean cacheAble;
    // used to store table's ID->datanodes cache
    // format is table.primaryKey
    private String primaryKey;
    // limit output total
    private int limitSize;
    private SQLMerge sqlMerge;

    private boolean callStatement = false; // 处理call关键字

    // 是否为全局表，只有在insert、update、delete、ddl里会判断并修改。默认不是全局表，用于修正全局表修改数据的反馈。
    private boolean globalTableFlag = false;

	// 是否完成了路由
	private boolean isFinishedRoute = false;

	// 是否完成了执行
	private boolean isFinishedExecute = false;

    //是否自动提交，此属性主要用于记录ServerConnection上的autocommit状态
	private boolean autocommit = true;

    private boolean isLoadData=false;

    //是否可以在从库运行,此属性主要供RouteResultsetNode获取
    private Boolean canRunInReadDB;

    // 强制走 master，可以通过 RouteResultset的属性canRunInReadDB=false
    // 传给 RouteResultsetNode 来实现，但是 强制走 slave需要增加一个属性来实现:
    private Boolean runOnSlave = null;	// 默认null表示不施加影响

    private NonBlockingSession session;

    public NonBlockingSession getSession() {
		return session;
	}

	public boolean isNeedOptimizer() {
		return needOptimizer;
	}

	public void setNeedOptimizer(boolean needOptimizer) {
		this.needOptimizer = needOptimizer;
	}

	public Boolean getRunOnSlave() {
		return runOnSlave;
	}

	public void setRunOnSlave(Boolean runOnSlave) {
		this.runOnSlave = runOnSlave;
	}
	  private Procedure procedure;

    public Procedure getProcedure()
    {
        return procedure;
    }

    public void setProcedure(Procedure procedure)
    {
        this.procedure = procedure;
    }

	public boolean isLoadData()
    {
        return isLoadData;
    }

    public void setLoadData(boolean isLoadData)
    {
        this.isLoadData = isLoadData;
    }

	public boolean isFinishedExecute() {
		return isFinishedExecute;
	}

	public void setFinishedExecute(boolean isFinishedExecute) {
		this.isFinishedExecute = isFinishedExecute;
	}
    public boolean isFinishedRoute() {
		return isFinishedRoute || needOptimizer;
    }

    public void setFinishedRoute(boolean isFinishedRoute) {
        this.isFinishedRoute = isFinishedRoute;
    }

    public boolean isGlobalTable() {
        return globalTableFlag;
    }

    public void setGlobalTable(boolean globalTableFlag) {
        this.globalTableFlag = globalTableFlag;
    }

    public RouteResultset(String stmt, int sqlType, NonBlockingSession session) {
        this.statement = stmt;
		this.srcStatement = stmt;
        this.limitSize = -1;
        this.sqlType = sqlType;
        this.session = session;
    }

    public void copyLimitToNodes() {

        if(nodes!=null)
        {
            for (RouteResultsetNode node : nodes)
            {
                if(node.getLimitSize()==-1&&node.getLimitStart()==0)
                {
                    node.setLimitStart(limitStart);
                    node.setLimitSize(limitSize);
                }
            }

        }
    }


    public SQLMerge getSqlMerge() {
        return sqlMerge;
    }

    public boolean isCacheAble() {
        return cacheAble;
    }

    public void setCacheAble(boolean cacheAble) {
        this.cacheAble = cacheAble;
    }

    public boolean needMerge() {
        return limitSize > 0 || sqlMerge != null;
    }

    public int getSqlType() {
        return sqlType;
    }

    public boolean isHasAggrColumn() {
        return (sqlMerge != null) && sqlMerge.isHasAggrColumn();
    }

    public int getLimitStart() {
        return limitStart;
    }

    public String[] getGroupByCols() {
        return (sqlMerge != null) ? sqlMerge.getGroupByCols() : null;
    }

    private SQLMerge createSQLMergeIfNull() {
        if (sqlMerge == null) {
            sqlMerge = new SQLMerge();
        }
        return sqlMerge;
    }

    public Map<String, Integer> getMergeCols() {
        return (sqlMerge != null) ? sqlMerge.getMergeCols() : null;
    }

    public void setLimitStart(int limitStart) {
        this.limitStart = limitStart;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKeyToCache() {
        return primaryKey != null;
    }

    public void setPrimaryKey(String primaryKey) {
        if (!primaryKey.contains(".")) {
            throw new java.lang.IllegalArgumentException(
                    "must be table.primarykey fomat :" + primaryKey);
        }
        this.primaryKey = primaryKey;
    }

    /**
     * return primary key items ,first is table name ,seconds is primary key
     *
     * @return
     */
    public String[] getPrimaryKeyItems() {
        return primaryKey.split("\\.");
    }

    public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
        if (orderByCols != null && !orderByCols.isEmpty()) {
            createSQLMergeIfNull().setOrderByCols(orderByCols);
//            this.setNeedOptimizer(true);
        }
    }

    public void setHasAggrColumn(boolean hasAggrColumn) {
        if (hasAggrColumn) {
            createSQLMergeIfNull().setHasAggrColumn(true);
//            this.setNeedOptimizer(true);
        }
    }

    public void setGroupByCols(String[] groupByCols) {
        if (groupByCols != null && groupByCols.length > 0) {
            createSQLMergeIfNull().setGroupByCols(groupByCols);
//            this.setNeedOptimizer(true);
        }
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        if (mergeCols != null && !mergeCols.isEmpty()) {
            createSQLMergeIfNull().setMergeCols(mergeCols);
//			this.setNeedOptimizer(true);
        }

    }

    public LinkedHashMap<String, Integer> getOrderByCols() {
        return (sqlMerge != null) ? sqlMerge.getOrderByCols() : null;

    }

    public String getStatement() {
        return statement;
    }

    public RouteResultsetNode[] getNodes() {
        return nodes;
    }

    public void setNodes(RouteResultsetNode[] nodes) {
        if(nodes!=null)
        {
           int nodeSize=nodes.length;
            for (RouteResultsetNode node : nodes)
            {
                node.setTotalNodeSize(nodeSize);
            }

        }
        this.nodes = nodes;
    }

    /**
     * @return -1 if no limit
     */
    public int getLimitSize() {
        return limitSize;
    }

    public void setLimitSize(int limitSize) {
        this.limitSize = limitSize;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public boolean isCallStatement() {
        return callStatement;
    }

    public void setCallStatement(boolean callStatement) {
        this.callStatement = callStatement;
        if(nodes!=null)
        {
            for (RouteResultsetNode node : nodes)
            {
                node.setCallStatement(callStatement);
            }

        }
    }

	public void changeNodeSqlAfterAddLimit(SchemaConfig schemaConfig, String sql, int offset, int count) {
		this.setStatement(sql);
		if (nodes != null) {
			for (RouteResultsetNode node : nodes) {
				node.setStatement(sql);
				node.setLimitStart(offset);
				node.setLimitSize(count);
			}
		}
	}

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public Boolean getCanRunInReadDB() {
        return canRunInReadDB;
    }

    public void setCanRunInReadDB(Boolean canRunInReadDB) {
        this.canRunInReadDB = canRunInReadDB;
    }

	public HavingCols getHavingCols() {
		return (sqlMerge != null) ? sqlMerge.getHavingCols() : null;
	}

	
	public void setHavings(HavingCols havings) {
		if (havings != null) {
			createSQLMergeIfNull().setHavingCols(havings);
		}
	}

	// Added by winbill, 20160314, for having clause, Begin ==>
	public void setHavingColsName(Object[] names) {
		if (names != null && names.length > 0) {
			createSQLMergeIfNull().setHavingColsName(names);
		}
	}
	// Added by winbill, 20160314, for having clause, End  <==

    public SQLStatement getSqlStatement() {
		return this.sqlStatement;
	}

	public void setSqlStatement(SQLStatement sqlStatement) {
		this.sqlStatement = sqlStatement;
	}


	@Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(srcStatement).append(", route={");
        if (nodes != null) {
            for (int i = 0; i < nodes.length; ++i) {
                s.append("\n ").append(FormatUtil.format(i + 1, 3));
                s.append(" -> ").append(nodes[i]);
            }
        }
        s.append("\n}");
        return s.toString();
    }
}
