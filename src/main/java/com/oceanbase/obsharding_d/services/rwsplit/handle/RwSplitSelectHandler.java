/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.rwsplit.handle;

import com.oceanbase.obsharding_d.server.parser.RwSplitServerParseSelect;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;


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
