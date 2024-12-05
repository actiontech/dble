/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.store.memalloc.MemSizeController;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Session {

    protected final AtomicBoolean isMultiStatement = new AtomicBoolean(false);
    protected volatile String remainingSql = null;

    /**
     * get frontend conn
     */
    public abstract FrontendConnection getSource();

    public void setHandlerStart(DMLResponseHandler handler) {

    }

    public void setHandlerEnd(DMLResponseHandler handler) {

    }

    public void onQueryError(byte[] message) {

    }

    public MemSizeController getJoinBufferMC() {
        return null;
    }

    public MemSizeController getOrderBufferMC() {
        return null;
    }

    public MemSizeController getOtherBufferMC() {
        return null;
    }

    public void setRouteResultToTrace(RouteResultsetNode[] nodes) {

    }

    public void allBackendConnReceive() {
    }

    public abstract void startFlowControl(int currentWritingSize);

    public abstract void stopFlowControl(int currentWritingSize);

    public abstract void releaseConnectionFromFlowControlled(BackendConnection con);

    public void multiStatementPacket(MySQLPacket packet) {
        if (this.isMultiStatement.get()) {
            packet.markMoreResultsExists();
        }
    }

    /**
     * reset the session multiStatementStatus
     */
    public void resetMultiStatementStatus() {
        //clear the record
        this.isMultiStatement.set(false);
        this.remainingSql = null;
    }

    public boolean generalNextStatement(String sql) {
        int index = ParseUtil.findNextBreak(sql);
        if (index + 1 < sql.length() && !ParseUtil.isEOF(sql, index)) {
            this.remainingSql = sql.substring(index + 1);
            this.isMultiStatement.set(true);
            return true;
        } else {
            this.remainingSql = null;
            this.isMultiStatement.set(false);
            return false;
        }
    }

    public AtomicBoolean getIsMultiStatement() {
        return isMultiStatement;
    }

    public String getRemainingSql() {
        return remainingSql;
    }

    public void setRemainingSql(String remainingSql) {
        this.remainingSql = remainingSql;
    }
}
