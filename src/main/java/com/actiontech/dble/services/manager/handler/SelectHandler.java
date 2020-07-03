/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.ManagerSelectHandler;
import com.actiontech.dble.services.manager.response.SelectMaxAllowedPacket;
import com.actiontech.dble.services.manager.response.SelectSessionTxReadOnly;
import com.actiontech.dble.services.manager.response.ShowSingleString;
import com.actiontech.dble.route.parser.ManagerParseSelect;
import com.actiontech.dble.server.response.SelectVersionComment;

import static com.actiontech.dble.route.parser.ManagerParseSelect.*;


public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseSelect.parse(stmt, offset);
        switch (rs & 0xff) {
            case VERSION_COMMENT:
                SelectVersionComment.response(service);
                break;
            case SESSION_TX_READ_ONLY:
                SelectSessionTxReadOnly.execute(service, stmt.substring(offset).trim());
                break;
            case SESSION_TRANSACTION_READ_ONLY:
                SelectSessionTxReadOnly.execute(service, stmt.substring(offset).trim());
                break;
            case MAX_ALLOWED_PACKET:
                SelectMaxAllowedPacket.execute(service);
                break;
            case TIMEDIFF:
                ShowSingleString.execute(service, stmt.substring(rs >>> 8), "00:00:00");
                break;
            default:
                (new ManagerSelectHandler()).execute(service, stmt);
        }

    }

}
