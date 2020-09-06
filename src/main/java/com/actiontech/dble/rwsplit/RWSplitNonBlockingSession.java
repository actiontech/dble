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

    public void execute(boolean canPushDown2Slave, Callback callback) throws IOException {
        if (conn != null && !conn.isClosed()) {
            new RWSplitHandler(rwSplitService, null, callback).connectionAcquired(conn);
            return;
        }
        PhysicalDbInstance instance = rwGroup.select(canPushDown2Slave);
        instance.getConnection(rwSplitService.getSchema(), new RWSplitHandler(rwSplitService, null, callback), null, false);
    }

    //    public void execute(boolean canPushDown2Slave, Callback callback, byte[] originPacket) throws IOException {
    //        if (conn != null && !conn.isClosed()) {
    //            new RWSplitHandler(rwSplitService, originPacket, callback).connectionAcquired(conn);
    //            return;
    //        }
    //        PhysicalDbInstance instance = rwGroup.select(canPushDown2Slave);
    //        instance.getConnection(rwSplitService.getSchema(), new RWSplitHandler(rwSplitService, originPacket, callback), null, false);
    //    }

    public void setRwGroup(PhysicalDbGroup rwGroup) {
        this.rwGroup = rwGroup;
    }

    public void bind(BackendConnection bindConn) {
        if (conn != null) {
            LOGGER.warn("");
        }
        this.conn = bindConn;
    }

    public void unbindIfSafe() {
        if (this.rwSplitService.isAutocommit() && !rwSplitService.isLocked()) {
            this.conn.release();
            this.conn = null;
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public RWSplitService getService() {
        return rwSplitService;
    }
}
