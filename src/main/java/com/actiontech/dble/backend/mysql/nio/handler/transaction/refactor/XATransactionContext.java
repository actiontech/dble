package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.ParticipantLogEntry;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class XATransactionContext {

    private NonBlockingSession session;
    private List<RouteResultsetNode> involvedRrns;
    private AbstractXAHandler handler;
    private String errMsg;
    private boolean isRollback;
    private int retryTimes = 1;
    private int backgroundRetryTimes = 1;
    private ConcurrentMap<Object, Long> xaOldThreadIds;

    public boolean isRollback() {
        return isRollback;
    }

    public int getBackgroundRetryTimes() {
        return backgroundRetryTimes;
    }

    public void incrBackgroundRetryTimes() {
        backgroundRetryTimes++;
    }

    public void setRollback(boolean rollback) {
        isRollback = rollback;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void incrRetryTimes() {
        retryTimes++;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public XATransactionContext(NonBlockingSession session, AbstractXAHandler handler) {
        this.session = session;
        this.handler = handler;
    }

    public ConcurrentMap<Object, Long> getXaOldThreadIds() {
        return xaOldThreadIds;
    }

    public String getSessionXaId() {
        return session.getSessionXaID();
    }

    public AbstractXAHandler getHandler() {
        return handler;
    }

    public void setHandler(AbstractXAHandler handler) {
        this.handler = handler;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void setSession(NonBlockingSession session) {
        this.session = session;
    }

    public List<RouteResultsetNode> getInvolvedRrns() {
        return involvedRrns;
    }

    public void clearInvolvedRrns() {
        involvedRrns = null;
    }

    public void initXALogEntry() {
        CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(),
                new ParticipantLogEntry[session.getTargetCount()], TxState.TX_STARTED_STATE);
        XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);

        List<RouteResultsetNode> rrns = new ArrayList<>(session.getTargetCount());
        int position = 0;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(handler);
            rrns.add(rrn);

            XAStateLog.initRecoveryLog(session.getSessionXaID(), position, (MySQLConnection) conn);
            position++;
        }
        involvedRrns = rrns;
        xaOldThreadIds = new ConcurrentHashMap<>(rrns.size());
    }
}
