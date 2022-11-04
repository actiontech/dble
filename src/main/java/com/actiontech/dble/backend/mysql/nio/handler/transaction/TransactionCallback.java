/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

/**
 * acting on the explicit transaction callback
 * main content: transaction status updates and statistic
 */
public interface TransactionCallback {
    void callback();
}
