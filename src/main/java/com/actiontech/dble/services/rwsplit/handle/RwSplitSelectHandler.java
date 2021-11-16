/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.server.parser.RwSplitServerParseSelect;
import com.actiontech.dble.server.response.SelectVariables;
import com.actiontech.dble.services.rwsplit.RWSplitService;


/**
 * @author mycat
 */
public final class RwSplitSelectHandler {
    private RwSplitSelectHandler() {
    }

    public static void handle(String stmt, RWSplitService service, int offset) {
        switch (RwSplitServerParseSelect.parse(stmt, offset)) {
            case RwSplitServerParseSelect.SELECT_VAR_ALL:
                SelectVariables.execute(service, stmt);
                break;
            default: {
                int rs2 = RwSplitServerParseSelect.parseSpecial(stmt);
                switch (rs2) {
                    case RwSplitServerParseSelect.LOCK_READ:
                        service.getSession().execute(true, null, false);
                        break;
                    default:
                        service.getSession().execute(null, null, false);
                        break;
                }
                break;
            }


        }
    }

}
