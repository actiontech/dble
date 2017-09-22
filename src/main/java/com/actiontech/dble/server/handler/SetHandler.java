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
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.SystemVariables;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SetTestJob;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

/**
 * SetHandler
 *
 * @author mycat
 * @author zhuam
 */
public final class SetHandler {
    private SetHandler() {
    }

    private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    public enum KeyType {
        SYNTAX_ERROR,
        XA,
        AUTOCOMMIT,
        NAMES,
        CHARSET,
        CHARACTER_SET_CLIENT,
        CHARACTER_SET_CONNECTION,
        CHARACTER_SET_RESULTS,
        COLLATION_CONNECTION,
        SYSTEM_VARIABLES,
        USER_VARIABLES,
        TX_READ_ONLY,
        TX_ISOLATION
    }

    public static void handle(String stmt, ServerConnection c, int offset) {
        if (!ParseUtil.isSpace(stmt.charAt(offset))) {
            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
        }
        try {
            String smt = convertCharsetKeyWord(stmt);
            List<Pair<KeyType, Pair<String, String>>> contextTask = new ArrayList<>();
            if (handleSetStatement(smt, c, contextTask) && contextTask.size() > 0) {
                setStmtCallback(stmt, c, contextTask);
            }
        } catch (SQLSyntaxErrorException e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }

    private static SQLStatement parseSQL(String stmt) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        try {
            return parser.parseStatement();
        } catch (Exception t) {
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }
    private static boolean handleSetStatement(String stmt, ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask) throws SQLSyntaxErrorException {
        SQLStatement statement = parseSQL(stmt);
        if (statement instanceof SQLSetStatement) {
            List<SQLAssignItem> assignItems = ((SQLSetStatement) statement).getItems();
            if (assignItems.size() == 1 && contextTask.size() == 0) {
                return handleSingleVariable(stmt, assignItems.get(0), c, contextTask);
            } else {
                return handleSetMultiStatement(assignItems, c, contextTask);
            }
        } else if (statement instanceof MySqlSetNamesStatement) {
            MySqlSetNamesStatement setNamesStatement = (MySqlSetNamesStatement) statement;
            if (contextTask.size() > 0 || stmt.contains(",")) {
                if (handleSetNamesInMultiStmt(c, setNamesStatement.getCharSet(), setNamesStatement.getCollate(), contextTask)) {
                    int index = stmt.indexOf(",");
                    String newStmt = "set " + stmt.substring(index + 1);
                    return handleSetStatement(newStmt, c, contextTask);
                } else {
                    return false;
                }
            } else {
                return handleSingleSetNames(stmt, c, setNamesStatement);
            }
        } else if (statement instanceof MySqlSetCharSetStatement) {
            MySqlSetCharSetStatement setCharSetStatement = (MySqlSetCharSetStatement) statement;
            if (contextTask.size() > 0 || stmt.contains(",")) {
                if (handleCharsetInMultiStmt(c, setCharSetStatement.getCharSet(), contextTask)) {
                    int index = stmt.indexOf(",");
                    String newStmt = "set " + stmt.substring(index + 1);
                    return handleSetStatement(newStmt, c, contextTask);
                } else {
                    return false;
                }
            } else {
                return handleSingleSetCharset(stmt, c, setCharSetStatement);
            }
        } else if (statement instanceof MySqlSetTransactionStatement) {
            return handleTransaction(c, (MySqlSetTransactionStatement) statement);
        } else {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, stmt + " is not recognized and ignored");
            return false;
        }
    }

    private static boolean handleSetNamesInMultiStmt(ServerConnection c, String charset, String collate, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String[] charsetInfo = checkSetNames(charset, collate);
        if (charsetInfo != null) {
            contextTask.add(new Pair<>(KeyType.NAMES, new Pair<>(charsetInfo[0], charsetInfo[1])));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set  '" + charset + " or collate '" + collate + "'");
            return false;
        }
    }

    private static boolean handleSingleSetNames(String stmt, ServerConnection c, MySqlSetNamesStatement statement) {
        String[] charsetInfo = checkSetNames(statement.getCharSet(), statement.getCollate());
        if (charsetInfo != null) {
            c.setNames(charsetInfo[0], charsetInfo[1]);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set in statement '" + stmt + "");
            return false;
        }
    }

    private static boolean handleSingleSetCharset(String stmt, ServerConnection c, MySqlSetCharSetStatement statement) {
        String charset = getCharset(statement.getCharSet());
        if (charset != null) {
            c.setCharacterSet(charset);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set in statement '" + stmt + "");
            return false;
        }
    }

    private static boolean handleSetMultiStatement(List<SQLAssignItem> assignItems, ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        for (SQLAssignItem assignItem : assignItems) {
            if (!handleVariableInMultiStmt(assignItem, c, contextTask)) {
                return false;
            }
        }
        return true;
    }

    //execute multiStmt and callback to reset conn
    private static void setStmtCallback(String multiStmt, ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        c.setContextTask(contextTask);
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SetCallBack(c));
        SetTestJob sqlJob = new SetTestJob(multiStmt, resultHandler, c);
        sqlJob.run();
    }

    private static boolean handleVariableInMultiStmt(SQLAssignItem assignItem, ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String key = handleSetKey(assignItem, c);
        if (key == null) {
            return false;
        }
        SQLExpr valueExpr = assignItem.getValue();
        if (!checkValue(valueExpr)) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + assignItem.getValue() + "'");
            return false;
        }
        KeyType keyType = parseKeyType(key, true, KeyType.SYSTEM_VARIABLES);
        switch (keyType) {
            case XA:
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set xa cmd can't used in multi-set statement");
                return false;
            case AUTOCOMMIT:
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set autocommit cmd can't used in multi-set statement");
                return false;
            case NAMES: {
                String charset = parseStringValue(valueExpr);
                //TODO:druid lost collation info
                if (!handleSetNamesInMultiStmt(c, charset, null, contextTask)) return false;
                break;
            }
            case CHARSET: {
                String charset = parseStringValue(valueExpr);
                if (!handleCharsetInMultiStmt(c, charset, contextTask)) return false;
                break;
            }
            case CHARACTER_SET_CLIENT:
                if (!handleCharsetClientInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case CHARACTER_SET_CONNECTION:
                if (!handleCharsetConnInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case CHARACTER_SET_RESULTS:
                if (!handleCharsetResultsInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case COLLATION_CONNECTION:
                if (!handleCollationConnInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case TX_READ_ONLY:
                if (!handleReadOnlyInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case TX_ISOLATION:
                if (!handleTxIsolationInMultiStmt(c, contextTask, valueExpr)) return false;
                break;
            case SYSTEM_VARIABLES:
                if (SystemVariables.getDefaultValue(key) == null) {
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "system variable " + key + " is not supported");
                }
                contextTask.add(new Pair<>(KeyType.SYSTEM_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                break;
            case USER_VARIABLES:
                contextTask.add(new Pair<>(KeyType.USER_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                break;
            default:
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, key + " is not supported");
                return false;
        }
        return true;
    }

    private static boolean handleCharsetInMultiStmt(ServerConnection c, String charset, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String charsetInfo = getCharset(charset);
        if (charsetInfo != null) {
            contextTask.add(new Pair<>(KeyType.CHARSET, new Pair<String, String>(charsetInfo, null)));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charset + "'");
            return false;
        }
    }

    private static boolean handleTxIsolationInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String value = parseStringValue(valueExpr);
        Integer txIsolation = getIsolationLevel(value);
        if (txIsolation == null) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Variable 'tx_isolation' can't be set to the value of '" + value + "'");
            return false;
        }
        contextTask.add(new Pair<>(KeyType.TX_ISOLATION, new Pair<String, String>(String.valueOf(txIsolation), null)));
        return true;
    }

    private static boolean handleReadOnlyInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'tx_read_only|transaction_read_only'");
            return false;
        } else if (switchStatus) {
            contextTask.add(new Pair<>(KeyType.TX_READ_ONLY, new Pair<String, String>("true", null)));
        } else {
            contextTask.add(new Pair<>(KeyType.TX_READ_ONLY, new Pair<String, String>("false", null)));
        }
        return true;
    }

    private static boolean handleCollationConnInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String collation = parseStringValue(valueExpr);
        if (checkCollation(collation)) {
            contextTask.add(new Pair<>(KeyType.COLLATION_CONNECTION, new Pair<String, String>(collation, null)));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_COLLATION, "Unknown collation '" + collation + "'");
            return false;
        }
    }

    private static boolean handleCharsetResultsInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetResult = parseStringValue(valueExpr);
        if (charsetResult.equals("null") || checkCharset(charsetResult)) {
            contextTask.add(new Pair<>(KeyType.CHARACTER_SET_RESULTS, new Pair<String, String>(charsetResult, null)));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetResult + "'");
            return false;
        }
    }

    private static boolean handleCharsetConnInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetConnection = parseStringValue(valueExpr);
        if (charsetConnection.equals("null")) {
            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_connection' can't be set to the value of 'NULL'");
            return false;
        }
        String collationName = CharsetUtil.getDefaultCollation(charsetConnection);
        if (collationName != null) {
            contextTask.add(new Pair<>(KeyType.CHARACTER_SET_CONNECTION, new Pair<String, String>(collationName, null)));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetConnection + "'");
            return false;
        }
    }

    private static boolean handleCharsetClientInMultiStmt(ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetClient = parseStringValue(valueExpr);
        if (charsetClient.equals("null")) {
            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'NULL'");
            return false;
        }
        if (checkCharset(charsetClient)) {
            contextTask.add(new Pair<>(KeyType.CHARACTER_SET_CLIENT, new Pair<String, String>(charsetClient, null)));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetClient + "'");
            return false;
        }
    }

    private static boolean handleSingleVariable(String stmt, SQLAssignItem assignItem, ServerConnection c, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String key = handleSetKey(assignItem, c);
        if (key == null) return false;
        SQLExpr valueExpr = assignItem.getValue();
        if (!checkValue(valueExpr)) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + assignItem.getValue() + "'");
            return false;
        }
        KeyType keyType = parseKeyType(key, true, KeyType.SYSTEM_VARIABLES);
        switch (keyType) {
            case XA:
                return handleSingleXA(c, valueExpr);
            case AUTOCOMMIT:
                return handleSingleAutocommit(stmt, c, valueExpr);
            case CHARACTER_SET_CLIENT:
                return handleSingleCharsetClient(c, valueExpr);
            case CHARACTER_SET_CONNECTION:
                return handleSingleCharsetConnection(c, valueExpr);
            case CHARACTER_SET_RESULTS:
                return handleSingleCharsetResults(c, valueExpr);
            case COLLATION_CONNECTION:
                return handleCollationConnection(c, valueExpr);
            case TX_READ_ONLY:
                return handleTxReadOnly(c, valueExpr);
            case TX_ISOLATION:
                return handleTxIsolation(c, valueExpr);
            case SYSTEM_VARIABLES:
                if (SystemVariables.getDefaultValue(key) == null) {
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "system variable " + key + " is not supported");
                    return false;
                }
                contextTask.add(new Pair<>(KeyType.SYSTEM_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                return true;
            case USER_VARIABLES:
                contextTask.add(new Pair<>(KeyType.USER_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                return true;
            default:
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, stmt + " is not supported");
                return false;
        }
    }

    private static boolean handleTxReadOnly(ServerConnection c, SQLExpr valueExpr) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'tx_read_only|transaction_read_only'");
            return false;
        } else if (switchStatus) {
            c.setSessionReadOnly(true);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.setSessionReadOnly(false);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        }
        return true;
    }

    private static boolean handleTxIsolation(ServerConnection c, SQLExpr valueExpr) {
        String value = parseStringValue(valueExpr);
        Integer txIsolation = getIsolationLevel(value);
        if (txIsolation == null) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Variable 'tx_isolation' can't be set to the value of '" + value + "'");
            return false;
        }
        c.setTxIsolation(txIsolation);
        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        return true;
    }

    private static Integer getIsolationLevel(String value) {
        switch (value) {
            case "read-uncommitted":
                return Isolations.READ_UNCOMMITTED;
            case "read-committed":
                return Isolations.READ_COMMITTED;
            case "repeatable-read":
                return Isolations.REPEATED_READ;
            case "serializable":
                return Isolations.SERIALIZABLE;
            default:
                return null;
        }
    }

    private static boolean handleCollationConnection(ServerConnection c, SQLExpr valueExpr) {
        String collation = parseStringValue(valueExpr);
        if (checkCollation(collation)) {
            c.setCollationConnection(collation);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_COLLATION, "Unknown collation '" + collation + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetResults(ServerConnection c, SQLExpr valueExpr) {
        String charsetResult = parseStringValue(valueExpr);
        if (charsetResult.equals("null") || checkCharset(charsetResult)) {
            c.setCharacterResults(charsetResult);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetResult + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetConnection(ServerConnection c, SQLExpr valueExpr) {
        String charsetConnection = parseStringValue(valueExpr);
        if (charsetConnection.equals("null")) {
            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_connection' can't be set to the value of 'NULL'");
            return false;
        }
        String collationName = CharsetUtil.getDefaultCollation(charsetConnection);
        if (collationName != null) {
            c.setCharacterConnection(collationName);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetConnection + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetClient(ServerConnection c, SQLExpr valueExpr) {
        String charsetClient = parseStringValue(valueExpr);
        if (charsetClient.equals("null")) {
            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'NULL'");
            return false;
        }
        if (checkCharset(charsetClient)) {
            c.setCharacterClient(charsetClient);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetClient + "'");
            return false;
        }
    }

    private static boolean handleSingleAutocommit(String stmt, ServerConnection c, SQLExpr valueExpr) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'AUTOCOMMIT'");
            return false;
        } else if (switchStatus) {
            if (c.isAutocommit()) {
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            } else {
                c.commit("commit[because of " + stmt + "]");
                c.setAutocommit(true);
            }
        } else {
            if (c.isAutocommit()) {
                c.setAutocommit(false);
                TxnLogHelper.putTxnLog(c, stmt);
            }
            c.write(c.writeToBuffer(AC_OFF, c.allocate()));
        }
        return true;
    }

    private static boolean handleSingleXA(ServerConnection c, SQLExpr valueExpr) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'XA'");
            return false;
        } else if (switchStatus) {
            if (c.isTxstart() && c.getSession2().getSessionXaID() == null) {
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set xa cmd on can't used before ending a transaction");
                return false;
            }
            c.getSession2().setXaTxEnabled(true);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            if (c.isTxstart() && c.getSession2().getSessionXaID() != null) {
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "set xa cmd off can't used before ending a transaction");
                return false;
            }
            c.getSession2().setXaTxEnabled(false);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        }
    }

    // druid not support 'set charset' ,change to 'set character set'
    private static String convertCharsetKeyWord(String stmt) {
        StringBuilder result = new StringBuilder();
        String toCheck = stmt.toLowerCase();
        int index = toCheck.indexOf("charset");
        int tailStart = 0;
        while (index > 0) {
            char before = toCheck.charAt(index - 1);
            char after = toCheck.charAt(index + 7);
            if ((ParseUtil.isSpace(before) || ',' == before) && ParseUtil.isSpace(after)) {
                result.append(stmt.substring(tailStart, index));
                result.append("character set");
            }
            tailStart = index + 7;
            index = toCheck.indexOf("charset", tailStart);
        }
        if (result.length() > 0) {
            result.append(stmt.substring(tailStart));
            return result.toString();
        }
        return stmt;
    }

    private static String handleSetKey(SQLAssignItem assignItem, ServerConnection c) {
        if (assignItem.getTarget() instanceof SQLPropertyExpr) {
            SQLPropertyExpr target = (SQLPropertyExpr) assignItem.getTarget();
            if (!(target.getOwner() instanceof SQLVariantRefExpr)) {
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + target + "'");
                return null;
            }
            SQLVariantRefExpr owner = (SQLVariantRefExpr) target.getOwner();
            if (owner.isGlobal()) {
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                return null;
            }
            return target.getName();
        } else if (assignItem.getTarget() instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr target = (SQLVariantRefExpr) assignItem.getTarget();
            if (target.isGlobal()) {
                c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                return null;
            }
            return target.getName();
        } else if (assignItem.getTarget() instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr target = (SQLIdentifierExpr) assignItem.getTarget();
            return target.getLowerName();
        } else {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + assignItem.getTarget() + "'");
            return null;
        }
    }

    private static boolean checkValue(SQLExpr valueExpr) {
        return (valueExpr instanceof SQLCharExpr) || (valueExpr instanceof SQLIdentifierExpr) ||
                (valueExpr instanceof SQLIntegerExpr);
    }

    private static KeyType parseKeyType(String key, boolean origin, KeyType defaultVariables) {
        switch (key.toLowerCase()) {
            case "xa":
                return KeyType.XA;
            case "autocommit":
                return KeyType.AUTOCOMMIT;
            case "collation_connection":
                return KeyType.COLLATION_CONNECTION;
            case "character_set_client":
                return KeyType.CHARACTER_SET_CLIENT;
            case "character_set_results":
                return KeyType.CHARACTER_SET_RESULTS;
            case "character_set_connection":
                return KeyType.CHARACTER_SET_CONNECTION;
            case "transaction_isolation":
            case "tx_isolation":
                return KeyType.TX_ISOLATION;
            case "transaction_read_only":
            case "tx_read_only":
                return KeyType.TX_READ_ONLY;
            case "names":
                return KeyType.NAMES;
            case "character set":
                return KeyType.CHARSET;
            default:
                if (!origin && key.startsWith("@")) {
                    return KeyType.SYNTAX_ERROR;
                } else if (key.startsWith("@@")) {
                    return parseKeyType(key.substring(2), false, KeyType.SYSTEM_VARIABLES);
                } else if (key.startsWith("@")) {
                    return parseKeyType(key.substring(1), false, KeyType.USER_VARIABLES);
                } else {
                    return defaultVariables;
                }
        }
    }

    private static Boolean isSwitchOn(SQLExpr valueExpr) {
        if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            int iValue = value.getNumber().intValue();
            if (iValue < 0 || iValue > 1) {
                return null;
            }
            return (iValue == 1);
        }
        String strValue = parseStringValue(valueExpr);
        switch (strValue) {
            case "on":
                return true;
            case "off":
                return false;
            default:
                return null;
        }
    }

    private static String parseVariablesValue(SQLExpr valueExpr) {
        String strValue = "";
        if (valueExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr value = (SQLIdentifierExpr) valueExpr;
            strValue = "'" + StringUtil.removeBackQuote(value.getSimpleName().toLowerCase()) + "'";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr value = (SQLCharExpr) valueExpr;
            strValue = "'" + value.getText().toLowerCase() + "'";
        } else if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            strValue = value.getNumber().toString();
        }
        return strValue;
    }

    private static String parseStringValue(SQLExpr valueExpr) {
        String strValue = "";
        if (valueExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr value = (SQLIdentifierExpr) valueExpr;
            strValue = StringUtil.removeBackQuote(value.getSimpleName().toLowerCase());
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr value = (SQLCharExpr) valueExpr;
            strValue = value.getText().toLowerCase();
        } else if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            strValue = value.getNumber().toString();
        }
        return strValue;
    }


    private static boolean handleTransaction(ServerConnection c, MySqlSetTransactionStatement setStatement) {
        //always single
        if (setStatement.getGlobal() == null) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting transaction without any SESSION or GLOBAL keyword is not supported now");
            return false;
        } else if (setStatement.getGlobal()) {
            c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
            return false;
        } else if (setStatement.getAccessModel() != null) {
            if (setStatement.getAccessModel().equals("ONLY")) {
                c.setSessionReadOnly(true);
            } else {
                c.setSessionReadOnly(false);
            }
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        } else {
            int txIsolation = Isolations.REPEATED_READ;
            switch (setStatement.getIsolationLevel()) {
                case "READ UNCOMMITTED":
                    txIsolation = Isolations.READ_UNCOMMITTED;
                    break;
                case "READ COMMITTED":
                    txIsolation = Isolations.READ_COMMITTED;
                    break;
                case "REPEATABLE READ":
                    txIsolation = Isolations.REPEATED_READ;
                    break;
                case "SERIALIZABLE":
                    txIsolation = Isolations.SERIALIZABLE;
                    break;
                default:
                    // can't be happened
                    break;
            }
            c.setTxIsolation(txIsolation);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            return true;
        }
    }

    private static boolean checkCollation(String collation) {
        int ci = CharsetUtil.getCollationIndex(collation);
        return ci > 0;
    }

    private static boolean checkCharset(String name) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        return ci > 0;
    }

    private static String getCharset(String charset) {
        charset = charset.toLowerCase();
        if (charset.equals("default")) {
            charset = DbleServer.getInstance().getConfig().getSystem().getCharset();
        }
        charset = StringUtil.removeApostropheOrBackQuote(charset);
        if (checkCharset(charset)) {
            return charset;
        }
        return null;
    }

    private static String[] checkSetNames(String charset, String collate) {
        charset = charset.toLowerCase();
        if (charset.equals("default")) {
            charset = DbleServer.getInstance().getConfig().getSystem().getCharset();
        } else {
            charset = StringUtil.removeApostropheOrBackQuote(charset);
            if (!checkCharset(charset)) {
                return null;
            }
        }
        if (collate == null) {
            collate = CharsetUtil.getDefaultCollation(charset);
        } else {
            collate = collate.toLowerCase();
            if (collate.equals("default")) {
                collate = CharsetUtil.getDefaultCollation(charset);
            } else if (CharsetUtil.getCollationIndex(collate) <= 0) {
                return null;
            }
        }
        return new String[]{charset, collate};
    }
}
