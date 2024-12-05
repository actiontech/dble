/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.ManagerSelectHandler;
import com.oceanbase.obsharding_d.services.manager.response.SelectMaxAllowedPacket;
import com.oceanbase.obsharding_d.services.manager.response.SelectSessionTxReadOnly;
import com.oceanbase.obsharding_d.services.manager.response.ShowSingleString;
import com.oceanbase.obsharding_d.route.parser.ManagerParseSelect;
import com.oceanbase.obsharding_d.server.response.SelectVersionComment;

import static com.oceanbase.obsharding_d.route.parser.ManagerParseSelect.*;


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
