/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParseSet;
import com.actiontech.dble.server.response.CharacterSet;
import com.actiontech.dble.util.SetIgnoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.actiontech.dble.server.parser.ServerParseSet.*;

/**
 * SetHandler
 *
 * @author mycat
 * @author zhuam
 */
public final class SetHandler {
    private SetHandler() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SetHandler.class);

    private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    public static void handle(String stmt, ServerConnection c, int offset) {
        //TODO: set split with ','
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
            case AUTOCOMMIT_ON:
                if (c.isAutocommit()) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.commit("commit[because of " + stmt + "]");
                    c.setAutocommit(true);
                }
                break;
            case AUTOCOMMIT_OFF: {
                if (c.isAutocommit()) {
                    c.setAutocommit(false);
                    TxnLogHelper.putTxnLog(c, stmt);
                }
                c.write(c.writeToBuffer(AC_OFF, c.allocate()));
                break;
            }
            case XA_FLAG_ON: {
                if (c.isTxstart() && c.getSession2().getSessionXaID() == null) {
                    c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set xa cmd on can't used before ending a transaction");
                    return;
                }
                c.getSession2().setXaTxEnabled(true);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case XA_FLAG_OFF: {
                if (c.isTxstart() && c.getSession2().getSessionXaID() != null) {
                    c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set xa cmd off can't used before ending a transaction");
                    return;
                }
                c.getSession2().setXaTxEnabled(false);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                return;
            }
            case TX_READ_UNCOMMITTED: {
                c.setTxIsolation(Isolations.READ_UNCOMMITTED);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_READ_COMMITTED: {
                c.setTxIsolation(Isolations.READ_COMMITTED);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_REPEATED_READ: {
                c.setTxIsolation(Isolations.REPEATED_READ);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_SERIALIZABLE: {
                c.setTxIsolation(Isolations.SERIALIZABLE);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case NAMES: {
                handleSetNames(stmt, c, rs);
                break;
            }
            case CHARACTER_SET_CLIENT:
            case CHARACTER_SET_CONNECTION:
            case CHARACTER_SET_RESULTS:
                CharacterSet.response(stmt, c, rs);
                break;
            case CHARACTER_SET_NAME: {
                //ONLY SUPPORT:SET CHARACTER SET 'utf8';
                String charset = stmt.substring(rs >>> 8).trim();
                if (charset.startsWith("'") && charset.endsWith("'")) {
                    charset = charset.substring(1, charset.length() - 1);
                }
                if (charset.equalsIgnoreCase("utf8") && c.setCharset(charset)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                }
                break;
            }
            default:
                boolean ignore = SetIgnoreUtil.isIgnoreStmt(stmt);
                if (!ignore) {
                    StringBuilder s = new StringBuilder();
                    String warn = stmt + " is not recoginized and ignored";
                    LOGGER.warn(s.append(c).append(warn).toString());
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, warn);
                } else {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                }
        }
    }

    private static void handleSetNames(String stmt, ServerConnection c, int rs) {
        String charset = stmt.substring(rs >>> 8).trim();
        int index = charset.indexOf(",");
        if (index > -1) {
            // support rails for SET NAMES utf8, @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483, @@SESSION.sql_mode = 'STRICT_ALL_TABLES'
            charset = charset.substring(0, index);
        }
        if (charset.startsWith("'") && charset.endsWith("'")) {
            charset = charset.substring(1, charset.length() - 1);
        }
        if (c.setCharset(charset)) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {

            /**
             * FIXME: SET NAMES 'utf8' COLLATE 'utf8_general_ci'
             */
            int beginIndex = stmt.toLowerCase().indexOf("names");
            int endIndex = stmt.toLowerCase().indexOf("collate");
            int collateName = stmt.toLowerCase().indexOf("'utf8_general_ci'");
            if (beginIndex > -1 && endIndex > -1 && collateName > -1) {
                charset = stmt.substring(beginIndex + "names".length(), endIndex);
                //try again
                if (c.setCharset(charset.trim())) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                }

            } else {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            }
        }
    }
}
