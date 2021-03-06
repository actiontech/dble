package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;

public final class XaHandler {

    private XaHandler() {
    }

    public static void xaStart(String stmt, RWSplitService service, int offset) {
        String xaId = StringUtil.removeAllApostrophe(stmt.substring(offset).trim());
        service.getSession().execute(true, (isSuccess, rwSplitService) -> {
            if (isSuccess) {
                StatisticListener.getInstance().record(service.getSession(), r -> r.onXaStart(xaId));
                rwSplitService.getAndIncrementTxId();
                StatisticListener.getInstance().record(service.getSession(), r -> r.onTxStart(service));
            }
        });
    }

    public static void xaFinish(RWSplitService service) {
        service.getSession().execute(true, (isSuccess, rwSplitService) -> {
            if (isSuccess) {
                StatisticListener.getInstance().record(service.getSession(), r -> r.onXaStop());
                StatisticListener.getInstance().record(service.getSession(), r -> r.onTxEnd());
            }
        });
    }
}
