/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParseSet;
import com.actiontech.dble.util.SetIgnoreUtil;
import com.actiontech.dble.util.SplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CharacterSet
 */

/**
 * @author mycat
 */
public final class CharacterSet {
    private CharacterSet() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CharacterSet.class);

    public static void response(String stmt, ServerConnection c, int rs) {
        /* FIXME: completely charsets support */
        if (-1 == stmt.indexOf(',')) {
            oneSetResponse(stmt, c, rs);
        } else {
            /* Focus on CHARACTER_SET_RESULTS,CHARACTER_SET_CONNECTION */
            multiSetResponse(stmt, c, rs);
        }
    }

    private static void oneSetResponse(String stmt, ServerConnection c, int rs) {
        if ((rs & 0xff) == ServerParseSet.CHARACTER_SET_CLIENT) {
            /* ignore client property */
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            String charset = stmt.substring(rs >>> 8).trim();
            if (charset.endsWith(";")) {
                /* the end is ;  */
                charset = charset.substring(0, charset.length() - 1);
            }

            if (charset.startsWith("'") || charset.startsWith("`")) {
                /* the charset in '' will not trim */
                charset = charset.substring(1, charset.length() - 1);
            }
            setCharset(charset, c);
        }
    }

    private static void multiSetResponse(String stmt, ServerConnection c, int rs) {
        String charResult = "null";
        String charConnection = "null";
        String[] sqlList = SplitUtil.split(stmt, ',', false);

        // check first
        switch (rs & 0xff) {
            case ServerParseSet.CHARACTER_SET_RESULTS:
                charResult = sqlList[0].substring(rs >>> 8).trim();
                break;
            case ServerParseSet.CHARACTER_SET_CONNECTION:
                charConnection = sqlList[0].substring(rs >>> 8).trim();
                break;
            default:
                break;
        }

        // check remaining
        for (int i = 1; i < sqlList.length; i++) {
            String sql = "set " + sqlList[i];
            if ((i + 1 == sqlList.length) && sql.endsWith(";")) {
                /* remove ';' in the end of the sql */
                sql = sql.substring(0, sql.length() - 1);
            }
            int rs2 = ServerParseSet.parse(sql, "set".length());
            switch (rs2 & 0xff) {
                case ServerParseSet.CHARACTER_SET_RESULTS:
                    charResult = sql.substring(rs2 >>> 8).trim();
                    break;
                case ServerParseSet.CHARACTER_SET_CONNECTION:
                    charConnection = sql.substring(rs2 >>> 8).trim();
                    break;
                case ServerParseSet.CHARACTER_SET_CLIENT:
                    break;
                default:
                    boolean ignore = SetIgnoreUtil.isIgnoreStmt(sql);
                    if (!ignore) {
                        StringBuilder s = new StringBuilder();
                        LOGGER.warn(s.append(c).append(sql).append(" is not executed").toString());
                    }
            }
        }

        if (charResult.startsWith("'") || charResult.startsWith("`")) {
            charResult = charResult.substring(1, charResult.length() - 1);
        }
        if (charConnection.startsWith("'") || charConnection.startsWith("`")) {
            charConnection = charConnection.substring(1, charConnection.length() - 1);
        }

        if ("null".equalsIgnoreCase(charResult)) {
            setCharset(charConnection, c);
            return;
        }
        if ("null".equalsIgnoreCase(charConnection)) {
            setCharset(charResult, c);
            return;
        }
        if (charConnection.equalsIgnoreCase(charResult)) {
            setCharset(charConnection, c);
        } else {
            String sb = "charset is not consistent:[connection=" + charConnection +
                    ",results=" + charResult + ']';
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, sb);
        }
    }

    private static void setCharset(String charset, ServerConnection c) {
        if ("null".equalsIgnoreCase(charset)) {
            /* ignore null */
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else if (c.setCharset(charset)) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            try {
                if (c.setCharsetIndex(Integer.parseInt(charset))) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
                }
            } catch (RuntimeException e) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
            }
        }
    }

}
