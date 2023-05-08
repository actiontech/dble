/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.connection;


import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.WriteOutTask;
import com.actiontech.dble.net.mysql.QuitPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLBackAuthService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.FlowController;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class BackendConnection extends PooledConnection {

    private long threadId = 0;

    private final ReadTimeStatusInstance instance;
    private final int flowHighLevel;
    private final int flowLowLevel;
    private volatile boolean backendWriteFlowControlled;

    private volatile String bindFront;

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, ResponseHandler handler, String schema) {
        super(channel, socketWR);
        this.instance = instance;
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
        this.flowLowLevel = config.getPoolConfig().getFlowLowLevel();
        this.flowHighLevel = config.getPoolConfig().getFlowHighLevel();
        this.lastTime = TimeUtil.currentTimeMillis();
        this.setService(new MySQLBackAuthService(this, config.getUser(), schema, config.getPassword(), null, handler));

    }

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) {
        super(channel, socketWR);
        this.instance = instance;
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
        this.flowLowLevel = config.getPoolConfig().getFlowLowLevel();
        this.flowHighLevel = config.getPoolConfig().getFlowHighLevel();
        this.lastTime = TimeUtil.currentTimeMillis();
        this.setService(new MySQLBackAuthService(this, config.getUser(), schema, config.getPassword(), listener, null));
    }


    @Override
    public void businessClose(String reason) {
        if (!(getService() instanceof AuthService)) {
            this.getBackendService().setResponseHandler(null);
            this.getService().cleanup();
        }
        this.getService().setFakeClosed(true);
        this.close(reason);
    }

    @Override
    public void setProcessor(IOProcessor processor) {
        this.processor = processor;
        processor.addBackend(this);
    }

    @Override
    public void stopFlowControl(int currentWritingSize) {
        if (backendWriteFlowControlled && currentWritingSize <= flowLowLevel) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("This connection stop flow control, currentWritingSize= {},the connection info is {}", currentWritingSize, this);
            backendWriteFlowControlled = false;
        }
    }

    @Override
    public void startFlowControl(int currentWritingSize) {
        if (!backendWriteFlowControlled && currentWritingSize > flowHighLevel) {
            LOGGER.debug("This connection start flow control, currentWritingSize= {}, the connection info is {}", currentWritingSize, this);
            backendWriteFlowControlled = true;
        }
    }

    public void enableRead() {
        if (frontWriteFlowControlled) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("This connection enableRead because of flow control, the connection info is {}", this);
            socketWR.enableRead();
            frontWriteFlowControlled = false;
        }
    }

    public void disableRead() {
        if (!frontWriteFlowControlled) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("This connection disableRead because of flow control, the connection info is {}", this);
            socketWR.disableRead();
            frontWriteFlowControlled = true;
        }
    }

    @Override
    public void release() {
        final MySQLResponseService service = getBackendService();
        if (service == null) {
            LOGGER.warn("the backend connection[{}] has been closed.", this);
        } else {
            service.release();
        }
        setBindFront(null);
    }

    @Override
    public synchronized void close(final String reason) {
        if (getCloseReason() == null || !getCloseReason().equals(reason))
            LOGGER.info("connection id " + id + " mysqlId " + threadId + " close for reason " + reason);
        boolean isAuthed = !this.getService().isFakeClosed() && !(this.getService() instanceof AuthService);
        if (!isClosed.get()) {
            if ((isAuthed || this.getService().isFakeClosed()) && channel.isOpen() && closeReason == null) {
                try {
                    closeGracefullyPassive(reason);
                } catch (Throwable e) {
                    LOGGER.info("error when try to quit the connection ,drop the error and close it anyway", e);
                    super.close(reason);
                    this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
                }
            } else {
                super.close(reason);
                if (isAuthed) {
                    this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
                }
            }
            if (isAuthed) {
                this.getBackendService().backendSpecialCleanUp();
            }
        } else {
            if (isAuthed) {
                this.cleanup(reason);
                this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
            } else {
                super.baseCleanup(reason);
            }
        }
    }


    @Override
    public synchronized void closeImmediately(final String reason) {
        LOGGER.info("connection id " + id + " mysqlId " + threadId + " close for reason " + reason);
        boolean isAuthed = !this.getService().isFakeClosed() && !(this.getService() instanceof AuthService);
        if (!isClosed.get()) {
            super.closeImmediately(reason);
            if (isAuthed) {
                this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
            }

            if (isAuthed) {
                this.getBackendService().backendSpecialCleanUp();
            }
        } else {
            if (isAuthed) {
                this.cleanup(reason);
                this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
            } else {
                super.baseCleanup(reason);
            }
        }
    }


    public boolean isBackendWriteFlowControlled() {
        return backendWriteFlowControlled;
    }

    public int getFlowHighLevel() {
        return flowHighLevel;
    }

    public int getFlowLowLevel() {
        return flowLowLevel;
    }

    public void closeWithFront(String reason) {
        if (getBackendService().getSession() != null) {
            getBackendService().getSession().getSource().close(reason);
        }
        this.close(reason);
    }


    private void closeGracefullyPassive(String reason) {
        this.closeReason = reason;
        writeClose(getService().writeToBuffer(QuitPacket.QUIT, allocate()));
    }

    private void writeClose(ByteBuffer buffer) {
        writeQueue.offer(new WriteOutTask(buffer, true));
        if (FlowController.isEnableFlowControl()) {
            writingSize.addAndGet(5);
        }
        this.socketWR.doNextWriteCheck();
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public void setBindFront(String bindFront) {
        this.bindFront = bindFront;
    }

    public MySQLResponseService getBackendService() {
        final AbstractService service = getService();
        return service instanceof MySQLResponseService ? (MySQLResponseService) service : null;
    }

    public ReadTimeStatusInstance getInstance() {
        return instance;
    }

    public boolean isFromSlaveDB() {
        return instance.isReadInstance();
    }

    @Override
    public String toString() { // show all
        return "BackendConnection[id = " + id + " host = " + host + " port = " + port + " localPort = " + localPort + " mysqlId = " + threadId + " db config = " + instance + (bindFront != null ? ", currentBindFrontend = " + bindFront : "") + "]";
    }

    // not show 'currentBindFrontend ='
    public String toString2() {
        return "BackendConnection[id = " + id + " host = " + host + " port = " + port + " localPort = " + localPort + " mysqlId = " + threadId + " db config = " + instance + "]";
    }
}
