/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route;

import com.actiontech.dble.sqlengine.mpp.HavingCols;
import com.actiontech.dble.util.FormatUtil;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mycat
 */
public final class RouteResultset implements Serializable {
    private static final long serialVersionUID = 3906972758236875720L;

    private String srcStatement; // origin statement
    private String statement;
    private String schema;
    private String table;
    private final int sqlType;
    private RouteResultsetNode[] nodes;
    private transient SQLStatement sqlStatement;

    private boolean needOptimizer;
    private int limitStart;
    private boolean cacheAble;
    // used to store table's ID->datanodes cache
    // format is table.primaryKey
    private String primaryKey;
    // limit output total
    private int limitSize;
    private SQLMerge sqlMerge;

    private boolean callStatement = false; // is Call Statement

    // used for insert. update. delete. ddl statement for affect rows.
    private boolean globalTableFlag = false;

    // FinishedRoute
    private boolean isFinishedRoute = false;

    // FinishedExecute
    private boolean isFinishedExecute = false;


    private boolean isLoadData = false;

    //canRunInReadDB,set from RouteResultsetNode
    private Boolean canRunInReadDB;

    // if force master,set canRunInReadDB=false
    // if force slave set runOnSlave,default null means not effect
    private Boolean runOnSlave = null;

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

    public Procedure getProcedure() {
        return procedure;
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }

    public boolean isLoadData() {
        return isLoadData;
    }

    public void setLoadData(boolean loadData) {
        this.isLoadData = loadData;
    }

    public boolean isFinishedExecute() {
        return isFinishedExecute;
    }

    public void setFinishedExecute(boolean finishedExecute) {
        this.isFinishedExecute = finishedExecute;
    }

    public boolean isFinishedRoute() {
        return isFinishedRoute || needOptimizer;
    }

    public void setFinishedRoute(boolean finishedRoute) {
        this.isFinishedRoute = finishedRoute;
    }

    public boolean isGlobalTable() {
        return globalTableFlag;
    }

    public void setGlobalTable(boolean globalTable) {
        this.globalTableFlag = globalTable;
    }

    public RouteResultset(String stmt, int sqlType) {
        this.statement = stmt;
        this.srcStatement = stmt;
        this.limitSize = -1;
        this.sqlType = sqlType;
    }

    public void copyLimitToNodes() {

        if (nodes != null) {
            for (RouteResultsetNode node : nodes) {
                if (node.getLimitSize() == -1 && node.getLimitStart() == 0) {
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


    public void setHasAggrColumn(boolean hasAggrColumn) {
        if (hasAggrColumn) {
            createSQLMergeIfNull().setHasAggrColumn(true);
        }
    }

    public void setGroupByCols(String[] groupByCols) {
        if (groupByCols != null && groupByCols.length > 0) {
            createSQLMergeIfNull().setGroupByCols(groupByCols);
        }
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        if (mergeCols != null && !mergeCols.isEmpty()) {
            createSQLMergeIfNull().setMergeCols(mergeCols);
        }

    }

    public LinkedHashMap<String, Integer> getOrderByCols() {
        return (sqlMerge != null) ? sqlMerge.getOrderByCols() : null;

    }


    public void setSrcStatement(String srcStatement) {
        this.srcStatement = srcStatement;
    }

    public String getSrcStatement() {
        return srcStatement;
    }

    public String getStatement() {
        return statement;
    }

    public RouteResultsetNode[] getNodes() {
        return nodes;
    }

    public void setNodes(RouteResultsetNode[] nodes) {
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
    }

    public void changeNodeSqlAfterAddLimit(String sql, int offset, int count) {
        this.setStatement(sql);
        if (nodes != null) {
            for (RouteResultsetNode node : nodes) {
                node.setStatement(sql);
                node.setLimitStart(offset);
                node.setLimitSize(count);
            }
        }
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
