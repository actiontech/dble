package io.mycat.backend.mysql.xa;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;

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
			LOGGER.debug("[CALLBACK][XA " + operator + "] when Mycat start");
			XAStateLog.updateXARecoverylog(logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), txState);
			XAStateLog.writeCheckpoint(logEntry.getCoordinatorId());
		} else {
			LOGGER.warn("[CALLBACK][XA " + operator + "] when Mycat start,but failed");
		}
	}
}
