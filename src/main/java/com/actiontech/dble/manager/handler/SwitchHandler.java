/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.SwitchDataSource;
import com.actiontech.dble.route.parser.ManagerParseSwitch;

/**
 * @author mycat
 */
public final class SwitchHandler {
    private SwitchHandler() {
    }

    public static void handler(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSwitch.parse(stmt, offset)) {
            case ManagerParseSwitch.DATASOURCE:
                SwitchDataSource.response(stmt, c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
