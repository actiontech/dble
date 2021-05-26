package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import java.sql.SQLException;

public class StageRecorder {

    private SQLException exception;

    public void setException(SQLException exception) {
        this.exception = exception;
    }

    public void check() throws SQLException {
        if (exception != null) {
            throw exception;
        }
    }
}
