/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
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
            txState = TxState.TX_COMMITTED_STATE;
        } else {
            operator = "ROLLBACK";
            txState = TxState.TX_ROLLBACKED_STATE;
        }

        if (LOGGER.isDebugEnabled()) {
            String prepareDelayTime = System.getProperty("XA_RECOVERY_DELAY");
            long delayTime = prepareDelayTime == null ? 0 : Long.parseLong(prepareDelayTime) * 1000;
            //if using the debug log & using the jvm xa delay properties action will be delay by properties
            if (delayTime > 0) {
                try {
                    LOGGER.debug("before xa  recovery sleep time = " + delayTime);
                    Thread.sleep(delayTime);
                    LOGGER.debug("before xa recovery sleep finished " + delayTime);
                } catch (Exception e) {
                    LOGGER.debug("before xa recovery sleep exception " + delayTime);
                }
            }
        }
        this.logEntry = logEntry;
    }

    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            LOGGER.debug("[CALLBACK][XA " + operator + "] when server start");
            XAStateLog.updateXARecoveryLog(logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), txState);
            XAStateLog.writeCheckpoint(logEntry.getCoordinatorId());
        } else {
            LOGGER.warn("[CALLBACK][XA " + logEntry.getCoordinatorId() + logEntry.getHost() + logEntry.getPort() + logEntry.getSchema() + txState + "] when server start,but failed");
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", "operator " + operator);
            AlertUtil.alertSelf(AlarmCode.XA_RECOVER_FAIL, Alert.AlertLevel.WARN,
                    "[CALLBACK][XA " + logEntry.getCoordinatorId() + logEntry.getHost() + logEntry.getPort() + logEntry.getSchema() + txState + "] when server start,but failed", labels);
        }
    }
}
