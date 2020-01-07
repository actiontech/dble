/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.statistic;

/**
 * @author mycat
 */
public class CommandCount {

    private long initDB;
    private long query;
    private long stmtPrepare;
    private long stmtSendLongData;
    private long stmtReset;
    private long stmtExecute;
    private long stmtClose;
    private long ping;
    private long kill;
    private long quit;
    private long heartbeat;
    private long other;

    public CommandCount() {

    }

    public void doInitDB() {
        ++initDB;
    }

    public long initDBCount() {
        return initDB;
    }

    public void doQuery() {
        ++query;
    }

    public long queryCount() {
        return query;
    }

    public void doStmtPrepare() {
        ++stmtPrepare;
    }

    public long stmtPrepareCount() {
        return stmtPrepare;
    }

    public void doStmtSendLongData() {
        ++stmtSendLongData;
    }

    public long stmtSendLongDataCount() {
        return stmtSendLongData;
    }

    public void doStmtReset() {
        ++stmtReset;
    }

    public long stmtResetCount() {
        return stmtReset;
    }

    public void doStmtExecute() {
        ++stmtExecute;
    }

    public long stmtExecuteCount() {
        return stmtExecute;
    }

    public void doStmtClose() {
        ++stmtClose;
    }

    public long stmtCloseCount() {
        return stmtClose;
    }

    public void doPing() {
        ++ping;
    }

    public long pingCount() {
        return ping;
    }

    public void doKill() {
        ++kill;
    }

    public long killCount() {
        return kill;
    }

    public void doQuit() {
        ++quit;
    }

    public long quitCount() {
        return quit;
    }

    public void doOther() {
        ++other;
    }

    public long heartbeat() {
        return heartbeat;
    }

    public void doHeartbeat() {
        ++heartbeat;
    }

    public long otherCount() {
        return other;
    }

}
