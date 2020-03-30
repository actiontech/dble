/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage.XAStage;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.XASessionCheck;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mycat
 */
public final class NIOProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger("NIOProcessor");

    private final String name;
    private final BufferPool bufferPool;
    private final ConcurrentMap<Long, FrontendConnection> frontends;
    private final ConcurrentMap<Long, BackendConnection> backends;
    private final CommandCount commands;
    private long netInBytes;
    private long netOutBytes;

    // after reload @@config_all ,old back ends connections stored in backends_old
    public static final ConcurrentLinkedQueue<BackendConnection> BACKENDS_OLD = new ConcurrentLinkedQueue<>();

    private AtomicInteger frontEndsLength = new AtomicInteger(0);

    public NIOProcessor(String name, BufferPool bufferPool) throws IOException {
        this.name = name;
        this.bufferPool = bufferPool;
        this.frontends = new ConcurrentHashMap<>();
        this.backends = new ConcurrentHashMap<>();
        this.commands = new CommandCount();
    }

    public String getName() {
        return name;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public int getWriteQueueSize() {
        int total = 0;
        for (FrontendConnection frontend : frontends.values()) {
            total += frontend.getWriteQueue().size();
        }
        for (BackendConnection back : backends.values()) {
            if (back instanceof MySQLConnection) {
                total += ((MySQLConnection) back).getWriteQueue().size();
            }
        }
        return total;

    }

    public CommandCount getCommands() {
        return this.commands;
    }

    public long getNetInBytes() {
        return this.netInBytes;
    }

    public void addNetInBytes(long bytes) {
        this.netInBytes += bytes;
    }

    public long getNetOutBytes() {
        return this.netOutBytes;
    }

    public void addNetOutBytes(long bytes) {
        this.netOutBytes += bytes;
    }

    public void addFrontend(FrontendConnection c) {
        this.frontends.put(c.getId(), c);
        this.frontEndsLength.incrementAndGet();
    }

    public ConcurrentMap<Long, FrontendConnection> getFrontends() {
        return this.frontends;
    }

    public int getFrontendsLength() {
        return this.frontEndsLength.get();
    }

    public void addBackend(BackendConnection c) {
        this.backends.put(c.getId(), c);
    }

    public ConcurrentMap<Long, BackendConnection> getBackends() {
        return this.backends;
    }

    public void checkBackendCons() {
        backendCheck();
    }

    public void checkFrontCons() {
        frontendCheck();
    }

    private void frontendCheck() {
        Iterator<Entry<Long, FrontendConnection>> it = frontends.entrySet().iterator();
        while (it.hasNext()) {
            FrontendConnection c = it.next().getValue();

            // remove empty conn
            if (c == null) {
                it.remove();
                this.frontEndsLength.decrementAndGet();
                continue;
            }
            // clean closed conn or check timeout
            if (c.isClosed()) {
                c.cleanup();
                it.remove();
                this.frontEndsLength.decrementAndGet();
            } else {
                // very important ,for some data maybe not sent
                checkConSendQueue(c);
                if (c instanceof ServerConnection && c.isIdleTimeout()) {
                    ServerConnection s = (ServerConnection) c;
                    String xaStage = s.getSession2().getTransactionManager().getXAStage();
                    if (xaStage != null) {
                        if (!xaStage.equals(XAStage.COMMIT_FAIL_STAGE) && !xaStage.equals(XAStage.ROLLBACK_FAIL_STAGE)) {
                            // Active/IDLE/PREPARED XA FrontendS will be rollbacked
                            s.close("Idle Timeout");
                            XASessionCheck.getInstance().addRollbackSession(s.getSession2());
                        }
                        continue;
                    }
                }
                c.idleCheck();
            }
        }
    }

    private void checkConSendQueue(AbstractConnection c) {
        // very important ,for some data maybe not sent
        if (!c.writeQueue.isEmpty()) {
            c.getSocketWR().doNextWriteCheck();
        }
    }

    private void backendCheck() {
        long sqlTimeout = DbleServer.getInstance().getConfig().getSystem().getSqlExecuteTimeout() * 1000L;
        Iterator<Entry<Long, BackendConnection>> it = backends.entrySet().iterator();
        while (it.hasNext()) {
            BackendConnection c = it.next().getValue();

            // remove empty
            if (c == null) {
                it.remove();
                continue;
            }
            //Active/IDLE/PREPARED XA backends will not be checked
            if (c instanceof MySQLConnection) {
                MySQLConnection m = (MySQLConnection) c;
                if (m.isClosed()) {
                    it.remove();
                    continue;
                }
                if (m.getXaStatus() != null && m.getXaStatus() != TxState.TX_INITIALIZE_STATE) {
                    continue;
                }
            }
            // close the conn which executeTimeOut
            if (!c.isDDL() && c.isBorrowed() && c.isExecuting() && c.getLastTime() < TimeUtil.currentTimeMillis() - sqlTimeout) {
                LOGGER.info("found backend connection SQL timeout ,close it " + c);
                c.close("sql timeout");
            }

            // clean closed conn or check time out
            if (c.isClosed()) {
                it.remove();
            } else {
                // very important ,for some data maybe not sent
                if (c instanceof AbstractConnection) {
                    checkConSendQueue((AbstractConnection) c);
                }
                c.idleCheck();
            }
        }
    }

    public void removeConnection(AbstractConnection con) {
        if (con instanceof BackendConnection) {
            this.backends.remove(con.getId());
        } else {
            this.frontends.remove(con.getId());
            this.frontEndsLength.decrementAndGet();
        }

    }

}
