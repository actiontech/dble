/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.server.parser.RwSplitServerParseSelect;
import com.actiontech.dble.services.rwsplit.RWSplitService;


public final class RwSplitSelectHandler {
    private RwSplitSelectHandler() {
    }

    public static void handle(String stmt, RWSplitService service, int offset) {
        switch (RwSplitServerParseSelect.parse(stmt, offset)) {
            case RwSplitServerParseSelect.SELECT_VAR_ALL:
                service.getSession2().selectCompatibilityVariables(true, null, null, false, false);
                break;
            default: {
                int rs2 = RwSplitServerParseSelect.parseSpecial(stmt);
                switch (rs2) {
                    case RwSplitServerParseSelect.LOCK_READ:
                        service.getSession2().execute(true, null, false, true);
                        break;
                    default:
                        service.getSession2().execute(null, null, false, true);
                        break;
                }
                break;
            }


        }
    }

}
