/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction;

/**
 * acting on the explicit transaction callback
 * main content: transaction status updates and statistic
 */
public interface TransactionCallback {
    void callback();
}
