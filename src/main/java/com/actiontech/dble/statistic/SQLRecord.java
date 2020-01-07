/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.statistic;

/**
 * @author mycat
 */
public final class SQLRecord implements Comparable<SQLRecord> {

    private String statement;
    private long startTime;
    private long executeTime;

    @Override
    public int compareTo(SQLRecord o) {
        long para = o.executeTime - executeTime;
        return (int) (para == 0 ? (o.startTime - startTime) : para);
    }

    @Override
    public boolean equals(Object arg0) {
        return super.equals(arg0);
    }

    @Override
    public int hashCode() {
        long hash = executeTime;
        hash = hash * 31 + startTime;
        return (int) hash;
    }


    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }
}
