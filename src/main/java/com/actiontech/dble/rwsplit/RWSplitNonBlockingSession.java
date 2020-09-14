package com.actiontech.dble.rwsplit;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RWSplitNonBlockingSession {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    public void execute(boolean master, Callback callback) throws IOException {
        execute(master, null, callback);
    }

    public void execute(boolean master, byte[] originPacket, Callback callback) throws IOException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback);
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            handler.execute(conn);
            return;
        }
        PhysicalDbInstance instance = rwGroup.select(master);
        instance.getConnection(rwSplitService.getSchema(), handler, null, false);
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

    public void unbindIfSafe() {
        if (rwSplitService.isAutocommit() && !rwSplitService.isLocked() &&
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
