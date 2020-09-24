package com.actiontech.dble.rwsplit;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;

public class RWSplitNonBlockingSession {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    public void execute(boolean master, Callback callback) {
        execute(master, null, callback);
    }

    public void execute(boolean master, byte[] originPacket, Callback callback) {
        try {
            RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback);
            if (conn != null && !conn.isClosed()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("select bind conn[id={}]", conn.getId());
                }
                checkDest(!conn.getInstance().isReadInstance());
                handler.execute(conn);
                return;
            }

            PhysicalDbInstance instance = rwGroup.select(master);
            checkDest(!instance.isReadInstance());
            instance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (IOException e) {
            LOGGER.warn("select conn error", e);
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        } catch (SQLSyntaxErrorException se) {
            rwSplitService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, se.getMessage());
        }
    }

    private void checkDest(boolean isMaster) throws SQLSyntaxErrorException {
        String dest = rwSplitService.getExpectedDest();
        if (dest == null) {
            return;
        }
        if (dest.equalsIgnoreCase("M") && isMaster) {
            return;
        }
        if (dest.equalsIgnoreCase("S") && !isMaster) {
            return;
        }
        throw new SQLSyntaxErrorException("unexpected dble_dest_expect,real[" + (isMaster ? "M" : "S") + "],expect[" + dest + "]");
    }

    public PhysicalDbGroup getRwGroup() {
        return rwGroup;
    }

    public void setRwGroup(PhysicalDbGroup rwGroup) {
        this.rwGroup = rwGroup;
    }

    public void bind(BackendConnection bindConn) {
        if (conn != null) {
            LOGGER.warn("last conn is remaining");
        }
        this.conn = bindConn;
    }

    public void unbindIfSafe(boolean safe) {
        if (safe) {
            this.conn.release();
            this.conn = null;
        } else {
            unbindIfSafe();
        }
    }

    public void unbindIfSafe() {
        if (rwSplitService.isAutocommit() && !rwSplitService.isTxStart() && !rwSplitService.isLocked() &&
                !rwSplitService.isTxStart() &&
                !rwSplitService.isInLoadData() &&
                !rwSplitService.isInPrepare()) {
            this.conn.release();
            this.conn = null;
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public void close(String reason) {
        if (conn != null) {
            conn.close(reason);
        }
    }

    public RWSplitService getService() {
        return rwSplitService;
    }
}
