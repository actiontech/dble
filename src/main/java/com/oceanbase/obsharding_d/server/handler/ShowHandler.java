/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.handler;


import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseShow;
import com.oceanbase.obsharding_d.server.response.*;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;


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
            case ServerParseShow.OBsharding_D_PROCESS_LIST:
                ShowOBsharding_DProcessList.response(service);
                break;
            case ServerParseShow.CHARSET:
                stmt = stmt.toLowerCase().replaceFirst("charset", "character set");
                // fallthrough
            default:
                service.execute(stmt, ServerParse.SHOW);
        }
    }
}
