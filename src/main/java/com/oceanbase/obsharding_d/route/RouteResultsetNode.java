/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route;

import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.sqlengine.mpp.LoadData;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author mycat
 */
public class RouteResultsetNode implements Serializable, Comparable<RouteResultsetNode> {

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
    //included table
    private Set<String> tableSet;
    private AtomicLong repeatTableIndex;
    private boolean isForUpdate = false;
    private volatile byte loadDataRrnStatus;
    private boolean nodeRepeat = false;

    public RouteResultsetNode(String name, int sqlType, String srcStatement) {
        this.name = name;
        this.limitStart = 0;
        this.limitSize = -1;
        this.sqlType = sqlType;
        this.statement = srcStatement;
        this.statementHash = srcStatement.hashCode();
        this.canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
        this.multiplexNum = new AtomicLong(0);
        this.repeatTableIndex = new AtomicLong(0);
        loadDataRrnStatus = 0;
    }

    public RouteResultsetNode(String name, int sqlType, String srcStatement, Set<String> tableSet) {
        this.name = name;
        this.limitStart = 0;
        this.limitSize = -1;
        this.sqlType = sqlType;
        this.statement = srcStatement;
        this.statementHash = srcStatement.hashCode();
        this.canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
        this.multiplexNum = new AtomicLong(0);
        this.repeatTableIndex = new AtomicLong(0);
        loadDataRrnStatus = 0;
        this.tableSet = tableSet;
    }

    public byte getLoadDataRrnStatus() {
        return loadDataRrnStatus;
    }

    public void setLoadDataRrnStatus(byte loadDataRrnStatus) {
        this.loadDataRrnStatus = loadDataRrnStatus;
    }

    public boolean isForUpdate() {
        return isForUpdate;
    }

    public void setForUpdate(boolean forUpdate) {
        isForUpdate = forUpdate;
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

    public AtomicLong getRepeatTableIndex() {
        return repeatTableIndex;
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

    public boolean isNodeRepeat() {
        return nodeRepeat;
    }

    public void setNodeRepeat(boolean nodeRepeat) {
        this.nodeRepeat = nodeRepeat;
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

    public void setTableSet(Set<String> tableSet) {
        this.tableSet = tableSet;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public Set<String> getTableSet() {
        return tableSet;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RouteResultsetNode) {
            RouteResultsetNode rrn = (RouteResultsetNode) obj;
            if (!((RouteResultsetNode) obj).isNodeRepeat()) {
                if ((this.multiplexNum.get() == rrn.getMultiplexNum().get()) && equals(name, rrn.getName())) {
                    return true;
                }
            } else {
                if (contains(rrn.getTableSet(), tableSet) && equals(name, rrn.getName()) && equals(rrn.getRepeatTableIndex().get(), repeatTableIndex.get())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean equals(Object str1, Object str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    public boolean contains(Set<String> source, Set<String> target) {
        if (source == null || target == null) {
            return source == target;
        }
        if (source.isEmpty()) {
            return target.isEmpty();
        }
        if (target.isEmpty()) {
            return source.isEmpty();
        }
        return source.containsAll(target) || target.containsAll(source);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("rrsNode[");
        sb.append(name);
        sb.append('-').append(nodeRepeat).append('-');
        if (null != tableSet && !tableSet.isEmpty()) {
            sb.append("{" + tableSet.stream().collect(Collectors.joining(",")) + "}." + repeatTableIndex + "-");
        }
        sb.append('{');
        sb.append(statement.length() <= 1024 ? statement : statement.substring(0, 1024) + "...");
        sb.append("}.");
        sb.append(multiplexNum.get());
        sb.append("]");
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
