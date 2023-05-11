/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.services.TransactionOperate;

public final class CallbackFactory {

    private CallbackFactory() {
    }

    public static final Callback DEFAULT = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.controlTx(TransactionOperate.QUERY);
    };

    public static final Callback TX_START = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.controlTx(TransactionOperate.BEGIN);
    };

    public static final Callback TX_END = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.controlTx(TransactionOperate.END);
    };

    public static final Callback TX_IMPLICITLYCOMMIT = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.implicitlyDeal();
    };

    public static final Callback TX_AUTOCOMMIT = (isSuccess, resp, rwSplitService) -> {
        if (!rwSplitService.isAutocommit()) {
            rwSplitService.getSession2().getConn().getBackendService().setAutocommit(true);
            rwSplitService.controlTx(TransactionOperate.AUTOCOMMIT);
        }
    };

    public static final Callback TX_UN_AUTOCOMMIT = (isSuccess, resp, rwSplitService) -> {
        if (rwSplitService.isAutocommit()) {
            rwSplitService.controlTx(TransactionOperate.UNAUTOCOMMIT);
        }
    };

    // public static final Callback XA_START = ...

    public static final Callback XA_END = (isSuccess, resp, rwSplitService) -> {
    };
}
