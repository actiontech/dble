/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
