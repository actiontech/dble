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
        int rs2 = RwSplitServerParseSelect.parseSpecial(stmt);
        if (rs2 == RwSplitServerParseSelect.LOCK_READ) {
            service.getSession2().execute(true, null, false, true);
        } else {
            service.getSession2().execute(null, null, false, true);
        }
    }

}
