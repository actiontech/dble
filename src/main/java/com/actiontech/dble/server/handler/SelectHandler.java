/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseSelect;
import com.actiontech.dble.server.response.*;


/**
 * @author mycat
 */
public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ServerConnection c, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c);
                break;
            case ServerParseSelect.USER:
                SelectUser.response(c);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c);
                break;
            case ServerParseSelect.SESSION_INCREMENT:
                SessionIncrement.response(c);
                break;
            case ServerParseSelect.SESSION_ISOLATION:
                SessionIsolation.response(c);
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
                SelectLastInsertId.response(c, stmt, offset);
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
                SelectIdentity.response(c, stmt, offset, orgName);
                break;
            case ServerParseSelect.SELECT_VAR_ALL:
                SelectVariables.execute(c, stmt);
                break;
            case ServerParseSelect.SESSION_TX_READ_ONLY:
                SelectTxReadOnly.response(c);
                break;
            default:
                c.execute(stmt, ServerParse.SELECT);
        }
    }

}
