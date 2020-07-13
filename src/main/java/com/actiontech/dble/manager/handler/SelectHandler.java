/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;


import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.ManagerSelectHandler;
import com.actiontech.dble.manager.response.SelectMaxAllowedPacket;
import com.actiontech.dble.manager.response.SelectSessionTxReadOnly;
import com.actiontech.dble.manager.response.ShowSingleString;
import com.actiontech.dble.route.parser.ManagerParseSelect;
import com.actiontech.dble.server.response.SelectVersionComment;

import static com.actiontech.dble.route.parser.ManagerParseSelect.*;


public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseSelect.parse(stmt, offset);
        switch (rs & 0xff) {
            case VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case SESSION_TX_READ_ONLY:
                SelectSessionTxReadOnly.execute(c, stmt.substring(offset).trim());
                break;
            case SESSION_TRANSACTION_READ_ONLY:
                SelectSessionTxReadOnly.execute(c, stmt.substring(offset).trim());
                break;
            case MAX_ALLOWED_PACKET:
                SelectMaxAllowedPacket.execute(c);
                break;
            case TIMEDIFF:
                ShowSingleString.execute(c, stmt.substring(rs >>> 8), "00:00:00");
                break;
            default:
                (new ManagerSelectHandler()).execute(c, stmt);
        }

    }

}
