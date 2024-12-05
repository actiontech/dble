/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.CheckFullMetaData;
import com.oceanbase.obsharding_d.services.manager.response.CheckGlobalConsistency;
import com.oceanbase.obsharding_d.services.manager.response.ShowTime;
import com.oceanbase.obsharding_d.route.parser.ManagerParseCheck;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;

/**
 * @author huqing.yan
 */
public final class CheckHandler {
    private CheckHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseCheck.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseCheck.META_DATA:
                ShowTime.execute(service, ProxyMeta.getInstance().getTmManager().getTimestamp());
                break;
            case ManagerParseCheck.FULL_META_DATA:
                CheckFullMetaData.execute(service, stmt);
                break;
            case ManagerParseCheck.GLOBAL_CONSISTENCY:
                CheckGlobalConsistency.execute(service, stmt);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
