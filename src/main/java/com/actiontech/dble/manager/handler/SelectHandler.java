/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;


import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.SelectMaxAllowedPacket;
import com.actiontech.dble.manager.response.SelectSessionAutoIncrement;
import com.actiontech.dble.manager.response.SelectSessionTxReadOnly;
import com.actiontech.dble.route.parser.ManagerParseSelect;
import com.actiontech.dble.server.response.SelectVersionComment;

import static com.actiontech.dble.route.parser.ManagerParseSelect.*;

public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSelect.parse(stmt, offset)) {
            case VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case SESSION_AUTO_INCREMENT:
                SelectSessionAutoIncrement.execute(c);
                break;
            case SESSION_TX_READ_ONLY:
                SelectSessionTxReadOnly.execute(c);
                break;
            case MAX_ALLOWED_PACKET:
                SelectMaxAllowedPacket.execute(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }

    }
}
