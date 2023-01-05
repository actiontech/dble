/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

public enum TransactionOperate {
    AUTOCOMMIT,
    UNAUTOCOMMIT,
    BEGIN,
    END, // commit、rollback
    IMPLICITLY_COMMIT, // == END
    QUERY,
    QUIT

}
