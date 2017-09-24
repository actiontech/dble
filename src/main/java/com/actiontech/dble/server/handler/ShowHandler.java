/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseShow;
import com.actiontech.dble.server.response.*;
import com.actiontech.dble.util.StringUtil;


/**
 * @author mycat
 */
public final class ShowHandler {
    private ShowHandler() {
    }

    public static void handle(String stmt, ServerConnection c, int offset) {

        // remove `
        stmt = StringUtil.replaceChars(stmt, "`", null);

        int type = ServerParseShow.parse(stmt, offset);
        switch (type) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;
            case ServerParseShow.TABLES:
                ShowTables.response(c, stmt);
                break;
            case ServerParseShow.COLUMNS:
                ShowColumns.response(c, stmt);
                break;
            case ServerParseShow.INDEX:
                ShowIndex.response(c, stmt);
                break;
            case ServerParseShow.CREATE_TABLE:
                ShowCreateTable.response(c, stmt);
                break;
            case ServerParseShow.CHARSET:
                stmt = stmt.toLowerCase().replaceFirst("charset", "character set");
                // fallthrough
            default:
                c.execute(stmt, ServerParse.SHOW);
        }
    }
}
