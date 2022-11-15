package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.services.TransactionOperate;
import com.actiontech.dble.statistic.sql.StatisticListener;

public final class CallbackFactory {

    private CallbackFactory() {
    }

    public static final Callback DEFAULT = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.controlTx(TransactionOperate.QUERY);
    };

    public static final Callback TX_START = (isSuccess, resp, rwSplitService) -> {
        if (rwSplitService.isInTransaction()) {
            StatisticListener.getInstance().record(rwSplitService, r -> r.onTxEnd());
            rwSplitService.controlTx(TransactionOperate.BEGIN);
            StatisticListener.getInstance().record(rwSplitService, r -> r.onTxStartByImplicitly());
        } else {
            rwSplitService.controlTx(TransactionOperate.BEGIN);
            StatisticListener.getInstance().record(rwSplitService, r -> r.onTxStart());
        }
    };

    public static final Callback TX_END = (isSuccess, resp, rwSplitService) -> {
        if (rwSplitService.isInTransaction()) {
            StatisticListener.getInstance().record(rwSplitService, r -> r.onTxEnd());
            if (!rwSplitService.isAutocommit()) {
                StatisticListener.getInstance().record(rwSplitService, r -> r.onTxStartByImplicitly());
            }
        }
        rwSplitService.controlTx(TransactionOperate.END);
    };

    public static final Callback TX_IMPLICITLYCOMMIT = (isSuccess, resp, rwSplitService) -> {
        rwSplitService.implicitlyDeal();
    };

    public static final Callback TX_AUTOCOMMIT = (isSuccess, resp, rwSplitService) -> {
        if (!rwSplitService.isAutocommit()) {
            StatisticListener.getInstance().record(rwSplitService, r -> r.onTxEnd());
            rwSplitService.getSession2().getConn().getBackendService().setAutocommit(true);
            rwSplitService.controlTx(TransactionOperate.AUTOCOMMIT);
        }
    };

    public static final Callback TX_UN_AUTOCOMMIT = (isSuccess, resp, rwSplitService) -> {
        if (rwSplitService.isAutocommit()) {
            if (!rwSplitService.isTxStart()) {
                StatisticListener.getInstance().record(rwSplitService, r -> r.onTxStart());
            }
            rwSplitService.controlTx(TransactionOperate.UNAUTOCOMMIT);
        }
    };

    // public static final Callback XA_START = ...

    public static final Callback XA_END = (isSuccess, resp, rwSplitService) -> {
        if (isSuccess) {
            StatisticListener.getInstance().record(rwSplitService.getSession(), r -> r.onXaStop());
        }
    };
}
