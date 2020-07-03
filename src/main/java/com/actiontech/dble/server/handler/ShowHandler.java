/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;


import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseShow;
import com.actiontech.dble.server.response.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;


/**
 * @author mycat
 */
public final class ShowHandler {
    private ShowHandler() {
    }

    public static void handle(String stmt, ShardingService service, int offset) {

        int type = ServerParseShow.parse(stmt, offset);
        switch (type) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(service);
                break;
            case ServerParseShow.TRACE:
                ShowTrace.response(service);
                break;
            case ServerParseShow.TABLES:
                ShowTables.response(service, stmt);
                break;
            case ServerParseShow.TABLE_STATUS:
                ShowTableStatus.response(service, stmt);
                break;
            case ServerParseShow.COLUMNS:
                ShowColumns.response(service, stmt);
                break;
            case ServerParseShow.INDEX:
                ShowIndex.response(service, stmt);
                break;
            case ServerParseShow.CREATE_TABLE:
                ShowCreateTable.response(service, stmt);
                break;
            case ServerParseShow.VARIABLES:
                ShowVariables.response(service, stmt);
                break;
            case ServerParseShow.CREATE_VIEW:
                ShowCreateView.response(service, stmt);
                break;
            case ServerParseShow.CREATE_DATABASE:
                ShowCreateDatabase.response(service, stmt);
                break;
            case ServerParseShow.CHARSET:
                stmt = stmt.toLowerCase().replaceFirst("charset", "character set");
                // fallthrough
            default:
                service.execute(stmt, ServerParse.SHOW);
        }
    }
}
