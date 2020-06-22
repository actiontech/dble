/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.handler.ManagerAuthenticator;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public class ManagerConnection extends FrontendConnection {

    private volatile boolean skipIdleCheck = false;
    private ManagerUserConfig userConfig;

    public ManagerConnection(NetworkChannel channel) throws IOException {
        super(channel);
        this.handler = new ManagerAuthenticator(this);
    }

    public ManagerUserConfig getUserConfig() {
        return userConfig;
    }

    public void setUserConfig(ManagerUserConfig userConfig) {
        this.userConfig = userConfig;
    }

    @Override
    public void handlerQuery(String sql) {
        // execute
        if (queryHandler != null) {
            queryHandler.setReadOnly(this.getUserConfig().isReadOnly());
            queryHandler.query(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Query unsupported!");
        }
    }

    @Override
    public boolean isIdleTimeout() {
        if (skipIdleCheck) {
            return false;
        }
        return super.isIdleTimeout();
    }

    @Override
    protected void setRequestTime() {
        //do nothing
    }

    @Override
    public void startProcess() {
        //do nothing
    }

    @Override
    public void markFinished() {
        //do nothing
    }

    @Override
    protected void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public void handle(final byte[] data) {
        handler.handle(data);
    }

    @Override
    public void startFlowControl(BackendConnection bcon) {

    }

    @Override
    public void stopFlowControl() {

    }

    @Override
    public void killAndClose(String reason) {
        this.close(reason);
    }

    public void skipIdleCheck(boolean skip) {
        this.skipIdleCheck = skip;
    }

}
