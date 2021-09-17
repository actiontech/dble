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
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class BackendConnection extends PooledConnection {

    private long threadId = 0;

    private ReadTimeStatusInstance instance;

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, ResponseHandler handler, String schema) {
        super(channel, socketWR);
        this.instance = instance;
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
        this.lastTime = TimeUtil.currentTimeMillis();
        this.setService(new MySQLBackAuthService(this, config.getUser(), schema, config.getPassword(), null, handler));

    }

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) {
        super(channel, socketWR);
        this.instance = instance;
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
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
    public void stopFlowControl() {
        LOGGER.info("This connection {} remove flow control", this);
        this.setFlowControlled(false);
    }

    @Override
    public void startFlowControl() {
        LOGGER.info("This connection {} begins flow control", this);
        this.setFlowControlled(true);
    }

    @Override
    public void release() {
        getBackendService().release();
    }

    @Override
    public synchronized void close(final String reason) {
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

    public void writeClose(ByteBuffer buffer) {
        writeQueue.offer(new WriteOutTask(buffer, true));

        this.socketWR.doNextWriteCheck();
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public MySQLResponseService getBackendService() {
        final AbstractService service = getService();
        return service instanceof MySQLResponseService ? (MySQLResponseService) service : null;
    }

    public ReadTimeStatusInstance getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "BackendConnection[id = " + id + " host = " + host + " port = " + port + " localPort = " + localPort + " mysqlId = " + threadId + " db config = " + instance;
    }
}
