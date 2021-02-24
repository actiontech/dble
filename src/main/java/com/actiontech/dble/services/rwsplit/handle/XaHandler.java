package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;

import java.util.Optional;

public final class XaHandler {

    private XaHandler() {
    }

    public static void xaStart(String stmt, RWSplitService service, int offset) {
        String xaId = StringUtil.removeAllApostrophe(stmt.substring(offset).trim());
        service.getSession().execute(true, (isSuccess, rwSplitService) -> {
            if (isSuccess) {
                Optional.ofNullable(StatisticListener.getInstance().getRecorder(service.getSession())).ifPresent(r -> r.onXaStart(xaId));
                rwSplitService.getAndIncrementTxId();
                Optional.ofNullable(StatisticListener.getInstance().getRecorder(service.getSession())).ifPresent(r -> r.onTxStartByBegin(service));
            }
        });
    }

    public static void xaFinish(RWSplitService service) {
        service.getSession().execute(true, (isSuccess, rwSplitService) -> {
            if (isSuccess) {
                Optional.ofNullable(StatisticListener.getInstance().getRecorder(service.getSession())).ifPresent(r -> r.onXaStop());
                Optional.ofNullable(StatisticListener.getInstance().getRecorder(service.getSession())).ifPresent(r -> r.onTxEndByCommit());
            }
        });
    }
}
