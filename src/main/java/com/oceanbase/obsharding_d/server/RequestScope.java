/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server;

import com.oceanbase.obsharding_d.backend.mysql.PreparedStatement;
import com.oceanbase.obsharding_d.server.variables.OutputStateEnum;

import java.io.Closeable;

/**
 * these variable only take effect in one request scope.
 * When new request coming,all of those will re-init.
 *
 * @author dcy
 */
public class RequestScope implements Closeable {
    private volatile OutputStateEnum outputState = OutputStateEnum.NORMAL_QUERY;
    private volatile boolean usingCursor = false;
    private volatile boolean prepared = false;
    private volatile boolean usingJoin = false;
    private volatile PreparedStatement currentPreparedStatement;


    public boolean isUsingJoin() {
        return usingJoin;
    }

    public void setUsingJoin(boolean usingJoin) {
        this.usingJoin = usingJoin;
    }

    public OutputStateEnum getOutputState() {
        return outputState;
    }

    public void setOutputState(OutputStateEnum outputState) {
        this.outputState = outputState;
    }

    public boolean isUsingCursor() {
        return usingCursor;
    }

    public void setUsingCursor(boolean usingCursor) {
        this.usingCursor = usingCursor;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }


    public PreparedStatement getCurrentPreparedStatement() {
        return currentPreparedStatement;
    }

    public void setCurrentPreparedStatement(PreparedStatement currentPreparedStatement) {
        this.currentPreparedStatement = currentPreparedStatement;
    }


    @Override
    public void close() {
        //recycle disk resource if needed.
    }


}
