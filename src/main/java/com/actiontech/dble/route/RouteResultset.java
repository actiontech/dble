/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route;

import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.io.Serializable;
import java.util.List;

/**
 * @author mycat
 */
public final class RouteResultset implements Serializable {
    private static final long serialVersionUID = 3906972758236875720L;

    private String srcStatement; // origin statement
    private String statement;
    private String schema;
    private String table;
    private String tableAlias;
    private final int sqlType;
    private RouteResultsetNode[] nodes;
    private transient SQLStatement sqlStatement;
    private DDLInfo.DDLType ddlType = DDLInfo.DDLType.UNKNOWN;

    private List<String> globalBackupNodes = null;

    private boolean needOptimizer;
    private int limitStart;
    private boolean cacheAble;
    // used to store table's ID->data nodes cache
    private String primaryKey;
    private boolean containsPrimaryFilter = false;
    // limit output total
    private int limitSize;

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
    private String[] groupByCols;

    public String[] getGroupByCols() {
        return groupByCols;
    }

    public void setGroupByCols(String[] groupByCols) {
        this.groupByCols = groupByCols;
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


    public boolean isCacheAble() {
        return cacheAble;
    }

    public void setCacheAble(boolean cacheAble) {
        this.cacheAble = cacheAble;
    }

    public int getSqlType() {
        return sqlType;
    }

    public int getLimitStart() {
        return limitStart;
    }

    public void setLimitStart(int limitStart) {
        this.limitStart = limitStart;
    }

    public boolean hasPrimaryKeyToCache() {
        return schema != null && table != null && primaryKey != null;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isContainsPrimaryFilter() {
        return containsPrimaryFilter;
    }

    public void setContainsPrimaryFilter(boolean containsPrimaryFilter) {
        this.containsPrimaryFilter = containsPrimaryFilter;
    }


    /**
     * return primary key items ,first is table name ,seconds is primary key
     */
    public String[] getPrimaryKeyItems() {
        return new String[]{StringUtil.getFullName(schema, table, '_'), primaryKey};
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

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
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


    public DDLInfo.DDLType getDdlType() {
        return ddlType;
    }

    public void setDdlType(DDLInfo.DDLType ddlType) {
        this.ddlType = ddlType;
    }

    public List<String> getGlobalBackupNodes() {
        return globalBackupNodes;
    }

    public void setGlobalBackupNodes(List<String> globalBackupNodes) {
        this.globalBackupNodes = globalBackupNodes;
    }

}
