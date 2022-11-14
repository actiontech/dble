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
