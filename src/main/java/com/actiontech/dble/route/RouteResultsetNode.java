/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.sqlengine.mpp.LoadData;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class RouteResultsetNode implements Serializable, Comparable<RouteResultsetNode> {

    private static final long serialVersionUID = 1L;
    private final String name; // node name
    private String statement; // the query for node to execute
    private int statementHash; // the query for node to execute
    private final int sqlType;
    private volatile boolean canRunInReadDB;
    private int limitStart;
    private int limitSize;
    private LoadData loadData;

    private Boolean runOnSlave = null;
    private AtomicLong multiplexNum;

    public RouteResultsetNode(String name, int sqlType, String srcStatement) {
        this.name = name;
        this.limitStart = 0;
        this.limitSize = -1;
        this.sqlType = sqlType;
        this.statement = srcStatement;
        this.statementHash = srcStatement.hashCode();
        this.canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
        this.multiplexNum = new AtomicLong(0);
    }

    public Boolean getRunOnSlave() {
        return runOnSlave;
    }

    public void setRunOnSlave(Boolean runOnSlave) {
        this.runOnSlave = runOnSlave;
    }

    public AtomicLong getMultiplexNum() {
        return multiplexNum;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public void setCanRunInReadDB(boolean canRunInReadDB) {
        this.canRunInReadDB = canRunInReadDB;
    }

    /**
     * <p>
     * if autocommit =0, can not use slave except use rwSplitMode hint.
     * <p>
     * of course, the query  is select or show (canRunInReadDB=true)
     *
     * @param autocommit
     * @return
     */
    public boolean canRunINReadDB(boolean autocommit) {
        return canRunInReadDB && (autocommit);
    }

    public String getName() {
        return name;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getStatement() {
        return statement;
    }

    public int getStatementHash() {
        return statementHash;
    }

    public int getLimitStart() {
        return limitStart;
    }

    public void setLimitStart(int limitStart) {
        this.limitStart = limitStart;
    }

    public int getLimitSize() {
        return limitSize;
    }

    public void setLimitSize(int limitSize) {
        this.limitSize = limitSize;
    }

    public LoadData getLoadData() {
        return loadData;
    }

    public void setLoadData(LoadData loadData) {
        this.loadData = loadData;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RouteResultsetNode) {
            RouteResultsetNode rrn = (RouteResultsetNode) obj;
            if ((this.multiplexNum.get() == rrn.getMultiplexNum().get()) && equals(name, rrn.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append('{');
        sb.append(statement.length() <= 1024 ? statement : statement.substring(0, 1024) + "...");
        sb.append("}.");
        sb.append(multiplexNum.get());
        return sb.toString();
    }

    public boolean isModifySQL() {
        return !canRunInReadDB;
    }

    @Override
    public int compareTo(RouteResultsetNode obj) {
        if (obj == null) {
            return 1;
        }
        if (this.name == null) {
            return -1;
        }
        if (obj.name == null) {
            return 1;
        }
        return this.name.compareTo(obj.name);
    }
}
