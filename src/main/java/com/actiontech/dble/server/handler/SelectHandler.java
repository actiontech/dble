/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseSelect;
import com.actiontech.dble.server.response.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;


/**
 * @author mycat
 */
public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ShardingService service, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(service);
                break;
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(service);
                break;
            case ServerParseSelect.USER:
                SelectUser.response(service);
                break;
            case ServerParseSelect.CURRENT_USER:
                SelectCurrentUser.response(service);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(service);
                break;
            case ServerParseSelect.SESSION_INCREMENT:
                SessionIncrement.response(service);
                break;
            case ServerParseSelect.SESSION_TX_ISOLATION:
                SessionIsolation.response(service, stmt.substring(offset).trim());
                break;
            case ServerParseSelect.SESSION_TRANSACTION_ISOLATION:
                SessionIsolation.response(service, stmt.substring(offset).trim());
                break;
            case ServerParseSelect.LAST_INSERT_ID:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop:
                for (int l = stmt.length(); offset < l; ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case 'L':
                        case 'l':
                            break loop;
                        default:
                            break;
                    }
                }
                offset = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectLastInsertId.response(service, stmt, offset);
                break;
            case ServerParseSelect.IDENTITY:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop:
                for (int l = stmt.length(); offset < l; ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case '@':
                            break loop;
                        default:
                            break;
                    }
                }
                int indexOfAtAt = offset;
                offset += 2;
                offset = ServerParseSelect.indexAfterIdentity(stmt, offset);
                String orgName = stmt.substring(indexOfAtAt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectIdentity.response(service, stmt, offset, orgName);
                break;
            case ServerParseSelect.SELECT_VAR_ALL:
                SelectVariables.execute(service, stmt);
                break;
            case ServerParseSelect.SESSION_TX_READ_ONLY:
                SelectTxReadOnly.response(service, stmt.substring(offset).trim());
                break;
            case ServerParseSelect.SESSION_TRANSACTION_READ_ONLY:
                SelectTxReadOnly.response(service, stmt.substring(offset).trim());
                break;
            case ServerParseSelect.TRACE:
                SelectTrace.response(service);
                break;
            case ServerParseSelect.ROW_COUNT:
                SelectRowCount.response(service);
                break;
            default:
                service.execute(stmt, ServerParse.SELECT);
        }
    }

}
