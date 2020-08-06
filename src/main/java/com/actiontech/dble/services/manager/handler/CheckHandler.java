/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.CheckFullMetaData;
import com.actiontech.dble.services.manager.response.CheckGlobalConsistency;
import com.actiontech.dble.services.manager.response.ShowTime;
import com.actiontech.dble.route.parser.ManagerParseCheck;
import com.actiontech.dble.singleton.ProxyMeta;

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
