/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

import static com.actiontech.dble.config.ErrorCode.ER_BAD_TABLE_ERROR;
import static com.actiontech.dble.config.ErrorCode.ER_PARSE_ERROR;

/**
 * Created by szf on 2017/10/17.
 */
public final class DropViewHandler {

    private DropViewHandler() {

    }

    public static void handle(String stmt, ServerConnection c) {
        try {
            boolean flag = parseViewExists(stmt);
            String[] viewName = parseViewName(stmt);

            if (viewName != null) {
                for (int i = 0; i < viewName.length; i++) {
                    viewName[i] = StringUtil.removeBackQuote(viewName[i]);
                }
            } else {
                c.writeErrMessage(ER_BAD_TABLE_ERROR, " no view in sql when try to drop");
                return;
            }
            //check if all the view is exists
            if (!flag) {
                for (String singleName : viewName) {
                    if (!(DbleServer.getInstance().getTmManager().getCatalogs().get(c.getSchema()).getViewMetas().containsKey(singleName))) {
                        c.writeErrMessage(ER_BAD_TABLE_ERROR, " Unknown table '" + singleName + "'");
                        return;
                    }
                }
            }

            for (String singleName : viewName) {
                DbleServer.getInstance().getTmManager().addMetaLock(c.getSchema(), singleName, stmt);
                try {
                    deleteFromReposoitory(c.getSchema(), singleName);
                    DbleServer.getInstance().getTmManager().getCatalogs().get(c.getSchema()).getViewMetas().remove(singleName.trim());
                } catch (Throwable e) {
                    throw e;
                } finally {
                    DbleServer.getInstance().getTmManager().removeMetaLock(c.getSchema(), singleName);
                }
            }


            byte packetId = (byte) c.getSession2().getPacketId().get();
            OkPacket ok = new OkPacket();
            ok.setPacketId(++packetId);
            c.getSession2().multiStatementPacket(ok, packetId);
            ok.write(c);
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.getSession2().multiStatementNextSql(multiStatementFlag);
            return;
        } catch (Exception e) {
            c.writeErrMessage(ER_PARSE_ERROR, "Get Error when delete the view");
        }
    }

    public static void deleteFromReposoitory(String schema, String name) {
        DbleServer.getInstance().getTmManager().getRepository().delete(schema, name);
    }


    public static String[] parseViewName(String sql) throws Exception {
        //skip the first word of sql 'drop '
        int offset = 4;
        String stmt = sql.trim();
        while (true) {
            if (!(stmt.charAt(++offset) == ' ' ||
                    stmt.charAt(offset) == '\t' ||
                    stmt.charAt(offset) == '\r' ||
                    stmt.charAt(offset) == '\n')) {
                //skip  the second words view
                offset += 4;
                while (true) {
                    char t = stmt.charAt(++offset);
                    if (!(t == ' ' || t == '\t' || t == '\r' || t == '\n')) {
                        if ((t == 'i' || t == 'I') &&
                                (stmt.charAt(++offset) == 'f' || stmt.charAt(offset) == 'F')) {
                            while (true) {
                                char t1 = stmt.charAt(++offset);
                                if (!(t1 == ' ' || t1 == '\t' || t1 == '\r' || t1 == '\n')) {
                                    char c1 = stmt.charAt(offset);
                                    char c2 = stmt.charAt(++offset);
                                    char c3 = stmt.charAt(++offset);
                                    char c4 = stmt.charAt(++offset);
                                    char c5 = stmt.charAt(++offset);
                                    char c6 = stmt.charAt(++offset);
                                    if ((c1 == 'e' || c1 == 'E') &&
                                            (c2 == 'x' || c2 == 'X') &&
                                            (c3 == 'i' || c3 == 'I') &&
                                            (c4 == 's' || c4 == 'S') &&
                                            (c5 == 't' || c5 == 'T') &&
                                            (c6 == 's' || c6 == 'S')) {
                                        return stmt.substring(offset + 1, stmt.length()).trim().split(",");
                                    }
                                    return null;
                                }
                            }
                        } else {
                            return stmt.substring(offset - 1, stmt.length()).trim().split(",");
                        }
                    }
                }
            }
        }
    }


    public static boolean parseViewExists(String sql) throws Exception {
        //skip the first word of sql 'drop '
        int offset = 4;
        String stmt = sql.trim();
        while (true) {
            if (!(stmt.charAt(++offset) == ' ' ||
                    stmt.charAt(offset) == '\t' ||
                    stmt.charAt(offset) == '\r' ||
                    stmt.charAt(offset) == '\n')) {
                //skip  the second words view
                offset += 4;
                while (true) {
                    char t = stmt.charAt(++offset);
                    if (!(t == ' ' || t == '\t' || t == '\r' || t == '\n')) {
                        if ((t == 'i' || t == 'I') &&
                                (stmt.charAt(++offset) == 'f' || stmt.charAt(offset) == 'F')) {
                            while (true) {
                                char t1 = stmt.charAt(++offset);
                                if (!(t1 == ' ' || t1 == '\t' || t1 == '\r' || t1 == '\n')) {
                                    char c1 = stmt.charAt(offset);
                                    char c2 = stmt.charAt(++offset);
                                    char c3 = stmt.charAt(++offset);
                                    char c4 = stmt.charAt(++offset);
                                    char c5 = stmt.charAt(++offset);
                                    char c6 = stmt.charAt(++offset);
                                    if ((c1 == 'e' || c1 == 'E') &&
                                            (c2 == 'x' || c2 == 'X') &&
                                            (c3 == 'i' || c3 == 'I') &&
                                            (c4 == 's' || c4 == 'S') &&
                                            (c5 == 't' || c5 == 'T') &&
                                            (c6 == 's' || c6 == 'S')) {
                                        return true;
                                    }
                                    return false;
                                }
                            }
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
    }
}
