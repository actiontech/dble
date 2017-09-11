/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangchao on 2016/10/18.
 */
public class XARecoverCallback implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(XARecoverCallback.class);
    private ParticipantLogEntry logEntry;
    private String operator;
    private TxState txState;

    public XARecoverCallback(boolean isCommit, ParticipantLogEntry logEntry) {
        if (isCommit) {
            operator = "COMMIT";
            txState = TxState.TX_COMMITED_STATE;
        } else {
            operator = "ROLLBACK";
            txState = TxState.TX_ROLLBACKED_STATE;
        }
        this.logEntry = logEntry;
    }

    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            LOGGER.debug("[CALLBACK][XA " + operator + "] when server start");
            XAStateLog.updateXARecoverylog(logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), txState);
            XAStateLog.writeCheckpoint(logEntry.getCoordinatorId());
        } else {
            LOGGER.warn("[CALLBACK][XA " + operator + "] when server start,but failed");
        }
    }
}
