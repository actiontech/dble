/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;

public interface TransactionHandler {
    void commit();
    void rollback();
    void setImplicitCommitHandler(ImplicitCommitHandler handler);
    void clearResources();
}
