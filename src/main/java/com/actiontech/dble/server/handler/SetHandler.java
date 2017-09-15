/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParseSet;
import com.actiontech.dble.util.SetIgnoreUtil;
import com.actiontech.dble.util.StringUtil;
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
        if (!ParseUtil.isSpace(stmt.charAt(offset))) {
            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
        }
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
            case MULTI_SET:
                //set split with ','
                if (!parserMultiSet(stmt.substring(offset), c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                }
                break;
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
            case TX_READ_WRITE:
                c.setSessionReadOnly(false);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case TX_READ_ONLY:
                c.setSessionReadOnly(true);
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case NAMES: {
                String names = stmt.substring(rs >>> 8).trim();
                if (handleSetNames(names, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set" + names + "");
                }
                break;
            }
            case CHARACTER_SET_CLIENT:
                String charsetClient = stmt.substring(rs >>> 8).trim().toLowerCase();
                if (charsetClient.equals("null")) {
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'NULL'");
                }
                if (handleCharSetClient(charsetClient, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + charsetClient + "");
                }
                break;
            case CHARACTER_SET_CONNECTION:
                String charsetConnection = stmt.substring(rs >>> 8).trim().toLowerCase();
                if (charsetConnection.equals("null")) {
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_connection' can't be set to the value of 'NULL'");
                }
                if (handleCharSetConnection(charsetConnection, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + charsetConnection + "");
                }
                break;
            case CHARACTER_SET_RESULTS:
                String charsetResult = stmt.substring(rs >>> 8).trim();
                if (handleCharSetResults(charsetResult, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + charsetResult + "");
                }
                break;
            case CHARACTER_SET_NAME: {
                String charset = stmt.substring(rs >>> 8).trim();
                if (handleCharSetName(charset, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + charset + "");
                }
                break;
            }
            case COLLATION_CONNECTION: {
                String collation = stmt.substring(rs >>> 8).trim();
                if (handleCollationConn(collation, c)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_COLLATION, "Unknown collation " + collation + "");
                }
                break;
            }
            case GLOBAL:
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                break;
            default:
                boolean ignore = SetIgnoreUtil.isIgnoreStmt(stmt);
                if (!ignore) {
                    StringBuilder s = new StringBuilder();
                    String warn = stmt + " is not recognized and ignored";
                    LOGGER.warn(s.append(c).append(warn).toString());
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, warn);
                } else {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                }
        }
    }

    //TODO:
    private static boolean parserMultiSet(String setSQL, ServerConnection c) {
        String[] setStatements = setSQL.split(",");
        for (String statement : setStatements) {
            boolean setError = false;
            int rs = ServerParseSet.parse(statement, 0);
            switch (rs & 0xff) {
                case AUTOCOMMIT_ON:

                    break;
                case AUTOCOMMIT_OFF: {
                    break;
                }
                case XA_FLAG_ON: {

                    break;
                }
                case XA_FLAG_OFF: {

                    break;
                }
                case NAMES: {
                    String names = statement.substring(rs >>> 8).trim();
                    if (!handleSetNames(names, c)) {
                        setError = true;
                        c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + names + "");
                    }
                    break;
                }
                case CHARACTER_SET_CLIENT:
                case CHARACTER_SET_CONNECTION:
                case CHARACTER_SET_RESULTS:
                    //TODO:
                    break;
                case CHARACTER_SET_NAME: {
                    String charset = statement.substring(rs >>> 8).trim();
                    if (!handleCharSetName(charset, c)) {
                        setError = true;
                        c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set " + charset + "");
                    }
                    break;
                }
                case GLOBAL:
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                    break;
                case TX_READ_UNCOMMITTED:
                case TX_READ_COMMITTED:
                case TX_REPEATED_READ:
                case TX_SERIALIZABLE: {
                    break;
                }
                default:
                    //TODO
            }
            if (setError) {
                return true;
            }
        }
        return false;
    }
    private static boolean handleCollationConn(String collation, ServerConnection c) {
        collation = StringUtil.removeApostropheOrBackQuote(collation);
        return c.setCollationConnection(collation);
    }
    private static boolean handleCharSetConnection(String charset, ServerConnection c) {
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        return c.setCharacterConnection(charset);
    }
    private static boolean handleCharSetResults(String charset, ServerConnection c) {
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        return c.setCharacterResults(charset);
    }
    private static boolean handleCharSetClient(String charset, ServerConnection c) {
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        return c.setCharacterClient(charset);
    }
    private static boolean handleCharSetName(String charset, ServerConnection c) {
        charset = charset.toLowerCase();
        if (charset.equals("default")) {
            charset = DbleServer.getInstance().getConfig().getSystem().getCharset();
        }
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        return c.setCharacterSet(charset);
    }

    private static boolean handleSetNames(String names, ServerConnection c) {
        String charset = names.toLowerCase();
        int collateIndex = charset.indexOf("collate");
        String collate = null;
        if (collateIndex > 0) {
            charset = names.substring(0, collateIndex).trim();
            collate = names.substring(collateIndex + 7).trim();
            if (collate.toLowerCase().equals("default")) {
                String defaultCharset = DbleServer.getInstance().getConfig().getSystem().getCharset();
                collate = CharsetUtil.getDefaultCollation(defaultCharset);
            }
        }
        if (charset.equals("default")) {
            charset = DbleServer.getInstance().getConfig().getSystem().getCharset();
        }
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        return c.setNames(charset, collate);

    }
}
