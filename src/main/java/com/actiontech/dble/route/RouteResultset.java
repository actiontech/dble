/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route;

import com.actiontech.dble.backend.mysql.nio.handler.ExecutableHandler;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.actiontech.dble.util.FormatUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author mycat
 */
public final class RouteResultset implements Serializable {

    private static final String DDL_TRACE_LOG = "DDL_TRACE";
    private static final Logger DTRACE_LOGGER = LoggerFactory.getLogger(DDL_TRACE_LOG);
    private static final long serialVersionUID = 3906972758236875720L;

    private String srcStatement; // origin statement
    private String statement;
    private String schema;
    private String table;
    private String tableAlias;
    private final int sqlType;
    private boolean online;
    private RouteResultsetNode[] nodes;
    private transient SQLStatement sqlStatement;
    private DDLInfo.DDLType ddlType = DDLInfo.DDLType.UNKNOWN;

    private List<String> globalBackupNodes = null;
    private boolean complexSQL = false;
    private boolean alwaysFalse = false;
    private boolean needOptimizer;
    private HintPlanInfo hintPlanInfo;
    private int limitStart;
    private boolean sqlRouteCacheAble;
    // limit output total
    private int limitSize;
    private transient ExecutableHandler ddlHandler;

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

    private Map<String, List<LoadDataRouteResultsetNode>> multiRouteResultSetNodeMap;

    private boolean isForUpdate = false;

    private boolean enableLoadDataFlag = false;

    public boolean isForUpdate() {
        return isForUpdate;
    }

    public void setForUpdate(boolean forUpdate) {
        isForUpdate = forUpdate;
    }

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
        if (DTRACE_LOGGER.isTraceEnabled()) {
            DTRACE_LOGGER.trace("the sql is need to use complex query Optimizer:" + srcStatement);
        }
    }

    public HintPlanInfo getHintPlanInfo() {
        return hintPlanInfo;
    }

    public void setHintPlanInfo(HintPlanInfo hintPlanInfo) {
        this.hintPlanInfo = hintPlanInfo;
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

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
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


    public boolean isSqlRouteCacheAble() {
        return sqlRouteCacheAble;
    }

    public void setSqlRouteCacheAble(boolean sqlRouteCacheAble) {
        this.sqlRouteCacheAble = sqlRouteCacheAble;
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
        if (alwaysFalse && !Objects.isNull(nodes) && nodes.length > 1) {
            if (DTRACE_LOGGER.isTraceEnabled()) {
                DTRACE_LOGGER.trace("the condition is always false ,route from broadcast to single");
            }
            RouteResultsetNode[] newNodes = new RouteResultsetNode[1];
            newNodes[0] = nodes[0];
            this.nodes = newNodes;
        } else {
            this.nodes = nodes;
        }
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
        s.append(srcStatement.length() > 1024 ? srcStatement.substring(0, 1024) + "..." : srcStatement).append(", route={");
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


    public boolean isComplexSQL() {
        return complexSQL;
    }

    public void setComplexSQL(boolean complexSQL) {
        this.complexSQL = complexSQL;
    }

    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public void setAlwaysFalse(boolean alwaysFalse) {
        this.alwaysFalse = alwaysFalse;
    }

    public Map<String, List<LoadDataRouteResultsetNode>> getMultiRouteResultSetNodeMap() {
        return multiRouteResultSetNodeMap;
    }

    public void setMultiRouteResultSetNodeMap(Map<String, List<LoadDataRouteResultsetNode>> multiRouteResultSetNodeMap) {
        this.multiRouteResultSetNodeMap = multiRouteResultSetNodeMap;
    }

    public boolean isEnableLoadDataFlag() {
        return enableLoadDataFlag;
    }

    public void setEnableLoadDataFlag(boolean enableLoadDataFlag) {
        this.enableLoadDataFlag = enableLoadDataFlag;
    }

    public ExecutableHandler getDdlHandler() {
        return ddlHandler;
    }

    public void setDdlHandler(ExecutableHandler ddlHandler) {
        this.ddlHandler = ddlHandler;
    }
}
