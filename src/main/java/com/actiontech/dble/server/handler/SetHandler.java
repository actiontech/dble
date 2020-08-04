/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SetTestJob;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * SetHandler
 *
 * @author mycat
 * @author zhuam
 */
public final class SetHandler {

    private SetHandler() {

    }

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
        TX_ISOLATION,
        TRACE
    }

    public static void handle(String stmt, ShardingService shardingService, int offset) {
        if (!ParseUtil.isSpace(stmt.charAt(offset))) {
            shardingService.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
        }
        try {
            String smt = convertCharsetKeyWord(stmt);
            List<Pair<KeyType, Pair<String, String>>> contextTask = new ArrayList<>();
            List<Pair<KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
            StringBuilder contextSetSQL = new StringBuilder();
            if (handleSetStatement(smt, shardingService, contextTask, innerSetTask, contextSetSQL) && contextTask.size() > 0) {
                setStmtCallback(contextSetSQL.toString(), shardingService, contextTask, innerSetTask);
            } else if (innerSetTask.size() > 0) {
                shardingService.setInnerSetTask(innerSetTask);
                if (!shardingService.executeInnerSetTask()) {
                    shardingService.write(shardingService.getSession2().getOKPacket());
                }
            }
        } catch (SQLSyntaxErrorException e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
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

    private static boolean handleSetStatement(String stmt, ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask,
                                              List<Pair<KeyType, Pair<String, String>>> innerSetTask, StringBuilder contextSetSQL) throws SQLSyntaxErrorException {
        SQLStatement statement = parseSQL(stmt);
        if (statement instanceof SQLSetStatement) {
            List<SQLAssignItem> assignItems = ((SQLSetStatement) statement).getItems();
            if (assignItems.size() == 1) {
                contextSetSQL.append(statement.toString());
                return handleSingleVariable(stmt, assignItems.get(0), service, contextTask);
            } else {
                boolean result = handleSetMultiStatement(assignItems, service, contextTask, innerSetTask);
                contextSetSQL.append(statement.toString());
                return result;
            }
        } else if (statement instanceof MySqlSetTransactionStatement) {
            return handleTransaction(service, (MySqlSetTransactionStatement) statement);
        } else {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, stmt + " is not recognized and ignored");
            return false;
        }
    }

    private static boolean handleSetNamesInMultiStmt(ShardingService service, String stmt, String charset, String collate, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        NamesInfo charsetInfo = checkSetNames(stmt, charset, collate);
        if (charsetInfo != null) {
            if (charsetInfo.charset == null) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set  '" + charset + " or collate '" + collate + "'");
                return false;
            } else if (charsetInfo.collation == null) {
                service.writeErrMessage(ErrorCode.ER_COLLATION_CHARSET_MISMATCH, "COLLATION '" + collate + "' is not valid for CHARACTER SET '" + charset + "'");
                return false;
            } else if (!charsetInfo.isSupport) {
                service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charsetInfo.charset + "'");
                return false;
            } else {
                contextTask.add(new Pair<>(KeyType.NAMES, new Pair<>(charsetInfo.charset, charsetInfo.collation)));
                return true;
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the SQL: " + stmt);
            return false;
        }
    }

    private static boolean handleSingleSetNames(String stmt, ShardingService shardingService, SQLExpr valueExpr) {
        String[] charsetAndCollate = parseNamesValue(valueExpr);
        NamesInfo charsetInfo = checkSetNames(stmt, charsetAndCollate[0], charsetAndCollate[1]);
        if (charsetInfo != null) {
            if (charsetInfo.charset == null) {
                shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set in statement '" + stmt + "'");
                return false;
            } else if (charsetInfo.collation == null) {
                shardingService.writeErrMessage(ErrorCode.ER_COLLATION_CHARSET_MISMATCH, "COLLATION '" + charsetAndCollate[1] + "' is not valid for CHARACTER SET '" + charsetAndCollate[0] + "'");
                return false;
            } else if (!charsetInfo.isSupport) {
                shardingService.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charsetInfo.charset + "'");
                return false;
            } else {
                shardingService.setNames(charsetInfo.charset, charsetInfo.collation);
                shardingService.write(shardingService.getSession2().getOKPacket());
                return true;
            }
        } else {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the SQL: " + stmt);
            return false;
        }
    }

    private static boolean handleSingleSetCharset(String stmt, ShardingService shardingService, SQLExpr valueExpr) {
        String charsetValue = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetValue == null || charsetValue.equalsIgnoreCase("null")) {
            shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set null");
            return false;
        }
        String charset = getCharset(charsetValue);
        if (charset != null) {
            if (!CharsetUtil.checkCharsetClient(charset)) {
                shardingService.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charset + "'");
                return false;
            } else {
                shardingService.setCharacterSet(charset);
                shardingService.write(shardingService.getSession2().getOKPacket());
                return true;
            }
        } else {
            shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set in statement '" + stmt + "'");
            return false;
        }
    }

    private static boolean handleSetMultiStatement(List<SQLAssignItem> assignItems, ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, List<Pair<KeyType, Pair<String, String>>> innerSetTask) {
        Set<SQLAssignItem> objSet = new HashSet<>();
        for (SQLAssignItem assignItem : assignItems) {
            if (!handleVariableInMultiStmt(assignItem, service, contextTask, innerSetTask, objSet)) {
                return false;
            }
        }
        for (SQLAssignItem assignItem : objSet) {
            assignItems.remove(assignItem);
        }
        return true;
    }

    //execute multiStmt and callback to reset conn
    private static void setStmtCallback(String multiStmt, ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, List<Pair<KeyType, Pair<String, String>>> innerSetTask) {
        service.setContextTask(contextTask);
        service.setInnerSetTask(innerSetTask);
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SetCallBack(service));
        SetTestJob sqlJob = new SetTestJob(multiStmt, null, resultHandler, service);
        sqlJob.run();
    }

    private static boolean handleVariableInMultiStmt(SQLAssignItem assignItem, ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, List<Pair<KeyType, Pair<String, String>>> innerSetTask, Set<SQLAssignItem> objSet) {
        String key = handleSetKey(assignItem, service);
        if (key == null) {
            return false;
        }
        SQLExpr valueExpr = assignItem.getValue();
        KeyType keyType = parseKeyType(key, true, KeyType.SYSTEM_VARIABLES);
        if (!checkValue(valueExpr, keyType)) {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + assignItem.getValue() + "'");
            return false;
        }
        switch (keyType) {
            case XA:
                if (!SetInnerHandler.preHandleSingleXA(service, valueExpr, innerSetTask)) {
                    return false;
                }
                objSet.add(assignItem);
                break;
            case TRACE:
                if (!SetInnerHandler.preHandleSingleTrace(service, valueExpr, innerSetTask)) {
                    return false;
                }
                objSet.add(assignItem);
                break;
            case AUTOCOMMIT:
                if (!SetInnerHandler.preHandleAutocommit(service, valueExpr, innerSetTask)) {
                    return false;
                }
                objSet.add(assignItem);
                break;
            case NAMES: {
                String charset = SetInnerHandler.parseStringValue(valueExpr);
                //TODO:druid lost collation info
                if (!handleSetNamesInMultiStmt(service, "SET NAMES " + charset, charset, null, contextTask))
                    return false;
                break;
            }
            case CHARSET: {
                String charset = SetInnerHandler.parseStringValue(valueExpr);
                if (!handleCharsetInMultiStmt(service, charset, contextTask)) return false;
                break;
            }
            case CHARACTER_SET_CLIENT:
                if (!handleCharsetClientInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case CHARACTER_SET_CONNECTION:
                if (!handleCharsetConnInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case CHARACTER_SET_RESULTS:
                if (!handleCharsetResultsInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case COLLATION_CONNECTION:
                if (!handleCollationConnInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case TX_READ_ONLY:
                if (!handleReadOnlyInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case TX_ISOLATION:
                if (!handleTxIsolationInMultiStmt(service, contextTask, valueExpr)) return false;
                break;
            case SYSTEM_VARIABLES:
                if (key.startsWith("@@")) {
                    key = key.substring(2);
                }
                if (DbleServer.getInstance().getSystemVariables().getDefaultValue(key) == null) {
                    service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "system variable " + key + " is not supported");
                }
                contextTask.add(new Pair<>(KeyType.SYSTEM_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                break;
            case USER_VARIABLES:
                contextTask.add(new Pair<>(KeyType.USER_VARIABLES, new Pair<>(key.toUpperCase(), parseVariablesValue(valueExpr))));
                break;
            default:
                service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, key + " is not supported");
                return false;
        }
        return true;
    }

    private static boolean handleCharsetInMultiStmt(ShardingService service, String charset, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String charsetInfo = getCharset(charset);
        if (charsetInfo != null) {
            if (!CharsetUtil.checkCharsetClient(charsetInfo)) {
                service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charsetInfo + "'");
                return false;
            } else {
                contextTask.add(new Pair<>(KeyType.CHARSET, new Pair<String, String>(charsetInfo, null)));
                return true;
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charset + "'");
            return false;
        }
    }

    private static boolean handleTxIsolationInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String value = SetInnerHandler.parseStringValue(valueExpr);
        Integer txIsolation = getIsolationLevel(value);
        if (txIsolation == null) {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Variable 'tx_isolation|transaction_isolation' can't be set to the value of '" + value + "'");
            return false;
        }
        contextTask.add(new Pair<>(KeyType.TX_ISOLATION, new Pair<String, String>(String.valueOf(txIsolation), null)));
        return true;
    }

    private static boolean handleReadOnlyInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        Boolean switchStatus = SetInnerHandler.isSwitchOn(valueExpr);
        if (switchStatus == null) {
            service.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'tx_read_only|transaction_read_only'");
            return false;
        } else if (switchStatus) {
            contextTask.add(new Pair<>(KeyType.TX_READ_ONLY, new Pair<String, String>("true", null)));
        } else {
            contextTask.add(new Pair<>(KeyType.TX_READ_ONLY, new Pair<String, String>("false", null)));
        }
        return true;
    }

    private static boolean handleCollationConnInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String collation = SetInnerHandler.parseStringValue(valueExpr);
        if (checkCollation(collation)) {
            contextTask.add(new Pair<>(KeyType.COLLATION_CONNECTION, new Pair<String, String>(collation, null)));
            return true;
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_COLLATION, "Unknown collation '" + collation + "'");
            return false;
        }
    }

    private static boolean handleCharsetResultsInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetResult = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetResult.equalsIgnoreCase("NULL") || checkCharset(charsetResult)) {
            contextTask.add(new Pair<>(KeyType.CHARACTER_SET_RESULTS, new Pair<String, String>(charsetResult, null)));
            return true;
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetResult + "'");
            return false;
        }
    }

    private static boolean handleCharsetConnInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetConnection = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetConnection.equals("null")) {
            service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_connection' can't be set to the value of 'NULL'");
            return false;
        }
        String collationName = CharsetUtil.getDefaultCollation(charsetConnection);
        if (collationName != null) {
            contextTask.add(new Pair<>(KeyType.CHARACTER_SET_CONNECTION, new Pair<String, String>(collationName, null)));
            return true;
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetConnection + "'");
            return false;
        }
    }

    private static boolean handleCharsetClientInMultiStmt(ShardingService service, List<Pair<KeyType, Pair<String, String>>> contextTask, SQLExpr valueExpr) {
        String charsetClient = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetClient.equalsIgnoreCase("null")) {
            service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'NULL'");
            return false;
        } else if (checkCharset(charsetClient)) {
            if (!CharsetUtil.checkCharsetClient(charsetClient)) {
                service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charsetClient + "'");
                return false;
            } else {
                contextTask.add(new Pair<>(KeyType.CHARACTER_SET_CLIENT, new Pair<String, String>(charsetClient, null)));
                return true;
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetClient + "'");
            return false;
        }
    }

    private static boolean handleSingleVariable(String stmt, SQLAssignItem assignItem, ShardingService shardingService, List<Pair<KeyType, Pair<String, String>>> contextTask) {
        String key = handleSetKey(assignItem, shardingService);
        if (key == null) return false;
        SQLExpr valueExpr = assignItem.getValue();
        KeyType keyType = parseKeyType(key, true, KeyType.SYSTEM_VARIABLES);
        if (!checkValue(valueExpr, keyType)) {
            shardingService.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + SQLUtils.toMySqlString(assignItem.getValue()) + "'");
            return false;
        }
        switch (keyType) {
            case NAMES:
                return handleSingleSetNames(stmt, shardingService, valueExpr);
            case CHARSET:
                return handleSingleSetCharset(stmt, shardingService, valueExpr);
            case XA:
                return SetInnerHandler.handleSingleXA(shardingService, valueExpr);
            case TRACE:
                return SetInnerHandler.handleSingleTrace(shardingService, valueExpr);
            case AUTOCOMMIT:
                return SetInnerHandler.handleSingleAutocommit(stmt, shardingService, valueExpr);
            case CHARACTER_SET_CLIENT:
                return handleSingleCharsetClient(shardingService, valueExpr);
            case CHARACTER_SET_CONNECTION:
                return handleSingleCharsetConnection(shardingService, valueExpr);
            case CHARACTER_SET_RESULTS:
                return handleSingleCharsetResults(shardingService, valueExpr);
            case COLLATION_CONNECTION:
                return handleCollationConnection(shardingService, valueExpr);
            case TX_READ_ONLY:
                if (!stmt.toLowerCase().contains("session")) {
                    shardingService.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting transaction without any SESSION or GLOBAL keyword is not supported now");
                    return false;
                }
                return handleTxReadOnly(shardingService, valueExpr);
            case TX_ISOLATION:
                if (!stmt.toLowerCase().contains("session")) {
                    shardingService.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting transaction without any SESSION or GLOBAL keyword is not supported now");
                    return false;
                }
                return handleTxIsolation(shardingService, valueExpr);
            case SYSTEM_VARIABLES:
                if (key.startsWith("@@")) {
                    key = key.substring(2);
                }
                if (DbleServer.getInstance().getSystemVariables().getDefaultValue(key) == null) {
                    shardingService.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "system variable " + key + " is not supported");
                    return false;
                }
                contextTask.add(new Pair<>(KeyType.SYSTEM_VARIABLES, new Pair<>(key, parseVariablesValue(valueExpr))));
                return true;
            case USER_VARIABLES:
                contextTask.add(new Pair<>(KeyType.USER_VARIABLES, new Pair<>(key.toUpperCase(), parseVariablesValue(valueExpr))));
                return true;
            default:
                shardingService.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, stmt + " is not supported");
                return false;
        }
    }

    private static boolean handleTxReadOnly(ShardingService shardingService, SQLExpr valueExpr) {
        Boolean switchStatus = SetInnerHandler.isSwitchOn(valueExpr);
        if (switchStatus == null) {
            shardingService.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'tx_read_only|transaction_read_only'");
            return false;
        } else if (switchStatus) {
            shardingService.setSessionReadOnly(true);
            shardingService.write(shardingService.getSession2().getOKPacket());
        } else {
            shardingService.setSessionReadOnly(false);
            shardingService.write(shardingService.getSession2().getOKPacket());
        }
        return true;
    }

    private static boolean handleTxIsolation(ShardingService service, SQLExpr valueExpr) {
        String value = SetInnerHandler.parseStringValue(valueExpr);
        Integer txIsolation = getIsolationLevel(value);
        if (txIsolation == null) {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "Variable 'tx_isolation|transaction_isolation' can't be set to the value of '" + value + "'");
            return false;
        }
        service.setTxIsolation(txIsolation);
        service.write(service.getSession2().getOKPacket());
        return true;
    }

    private static Integer getIsolationLevel(String value) {
        switch (value) {
            case "read-uncommitted":
                return Isolations.READ_UNCOMMITTED;
            case "read-committed":
                return Isolations.READ_COMMITTED;
            case "repeatable-read":
                return Isolations.REPEATABLE_READ;
            case "serializable":
                return Isolations.SERIALIZABLE;
            default:
                return null;
        }
    }

    private static boolean handleCollationConnection(ShardingService service, SQLExpr valueExpr) {
        String collation = SetInnerHandler.parseStringValue(valueExpr);
        if (checkCollation(collation)) {
            service.setCollationConnection(collation);
            service.write(service.getSession2().getOKPacket());
            return true;
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_COLLATION, "Unknown collation '" + collation + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetResults(ShardingService shardingService, SQLExpr valueExpr) {
        String charsetResult = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetResult.equalsIgnoreCase("NULL") || checkCharset(charsetResult)) {
            shardingService.setCharacterResults(charsetResult);
            shardingService.write(shardingService.getSession2().getOKPacket());
            return true;
        } else {
            shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetResult + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetConnection(ShardingService shardingService, SQLExpr valueExpr) {
        String charsetConnection = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetConnection.equals("null")) {
            shardingService.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_connection' can't be set to the value of 'NULL'");
            return false;
        }
        String collationName = CharsetUtil.getDefaultCollation(charsetConnection);
        if (collationName != null) {
            shardingService.setCharacterConnection(collationName);
            shardingService.write(shardingService.getSession2().getOKPacket());
            return true;
        } else {
            shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetConnection + "'");
            return false;
        }
    }

    private static boolean handleSingleCharsetClient(ShardingService service, SQLExpr valueExpr) {
        String charsetClient = SetInnerHandler.parseStringValue(valueExpr);
        if (charsetClient.equalsIgnoreCase("null")) {
            service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'NULL'");
            return false;
        }
        if (checkCharset(charsetClient)) {
            if (!CharsetUtil.checkCharsetClient(charsetClient)) {
                service.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of '" + charsetClient + "'");
                return false;
            } else {
                service.setCharacterClient(charsetClient);
                service.write(service.getSession2().getOKPacket());
                return true;
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown character set '" + charsetClient + "'");
            return false;
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

    private static String handleSetKey(SQLAssignItem assignItem, ShardingService service) {
        if (assignItem.getTarget() instanceof SQLPropertyExpr) {
            SQLPropertyExpr target = (SQLPropertyExpr) assignItem.getTarget();
            if (!(target.getOwner() instanceof SQLVariantRefExpr)) {
                service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + target + "'");
                return null;
            }
            SQLVariantRefExpr owner = (SQLVariantRefExpr) target.getOwner();
            if (owner.isGlobal()) {
                service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                return null;
            }
            return target.getName();
        } else if (assignItem.getTarget() instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr target = (SQLVariantRefExpr) assignItem.getTarget();
            if (target.isGlobal()) {
                service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
                return null;
            }
            return target.getName();
        } else if (assignItem.getTarget() instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr target = (SQLIdentifierExpr) assignItem.getTarget();
            return target.getLowerName();
        } else {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting target is not supported for '" + assignItem.getTarget() + "'");
            return null;
        }
    }

    private static boolean checkValue(SQLExpr valueExpr, KeyType keyType) {
        if (keyType == KeyType.USER_VARIABLES) {
            return !(valueExpr instanceof SQLQueryExpr);
        }
        return (valueExpr instanceof MySqlCharExpr) || (valueExpr instanceof SQLCharExpr) ||
                (valueExpr instanceof SQLIdentifierExpr) || (valueExpr instanceof SQLIntegerExpr) ||
                (valueExpr instanceof SQLNumberExpr) || (valueExpr instanceof SQLBooleanExpr) ||
                (valueExpr instanceof SQLDefaultExpr) || (valueExpr instanceof SQLNullExpr);
    }

    private static KeyType parseKeyType(String key, boolean origin, KeyType defaultVariables) {
        switch (key.toLowerCase()) {
            case "xa":
                return KeyType.XA;
            case "trace":
                return KeyType.TRACE;
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
            case VersionUtil.TRANSACTION_ISOLATION:
            case VersionUtil.TX_ISOLATION:
                return KeyType.TX_ISOLATION;
            case VersionUtil.TRANSACTION_READ_ONLY:
            case VersionUtil.TX_READ_ONLY:
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


    private static String parseVariablesValue(SQLExpr valueExpr) {
        String strValue;
        if (valueExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr value = (SQLIdentifierExpr) valueExpr;
            strValue = "'" + StringUtil.removeBackQuote(value.getSimpleName().toLowerCase()) + "'";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr value = (SQLCharExpr) valueExpr;
            strValue = "'" + value.getText().toLowerCase() + "'";
        } else if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            strValue = value.getNumber().toString();
        } else if (valueExpr instanceof SQLNumberExpr) {
            SQLNumberExpr value = (SQLNumberExpr) valueExpr;
            strValue = value.getNumber().toString();
        } else if (valueExpr instanceof SQLBooleanExpr) {
            SQLBooleanExpr value = (SQLBooleanExpr) valueExpr;
            strValue = String.valueOf(value.getValue());
        } else if (valueExpr instanceof SQLDefaultExpr || valueExpr instanceof SQLNullExpr) {
            strValue = valueExpr.toString();
        } else {
            strValue = SQLUtils.toMySqlString(valueExpr);
        }
        return strValue;
    }

    private static String[] parseNamesValue(SQLExpr valueExpr) {
        if (valueExpr instanceof MySqlCharExpr) {
            MySqlCharExpr value = (MySqlCharExpr) valueExpr;
            return new String[]{value.getText().toLowerCase(), StringUtil.removeBackQuote(value.getCollate()).toLowerCase()};
        } else {
            String charset = SetInnerHandler.parseStringValue(valueExpr);
            return new String[]{charset, null};
        }
    }


    private static boolean handleTransaction(ShardingService service, MySqlSetTransactionStatement setStatement) {
        //always single
        if (setStatement.getGlobal() == null) {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting transaction without any SESSION or GLOBAL keyword is not supported now");
            return false;
        } else if (setStatement.getGlobal()) {
            service.writeErrMessage(ErrorCode.ERR_NOT_SUPPORTED, "setting GLOBAL value is not supported");
            return false;
        } else if (setStatement.getAccessModel() != null) {
            if (setStatement.getAccessModel().equals("ONLY")) {
                service.setSessionReadOnly(true);
            } else {
                service.setSessionReadOnly(false);
            }
            service.write(service.getSession2().getOKPacket());
            return true;
        } else {
            int txIsolation = Isolations.REPEATABLE_READ;
            switch (setStatement.getIsolationLevel()) {
                case "READ UNCOMMITTED":
                    txIsolation = Isolations.READ_UNCOMMITTED;
                    break;
                case "READ COMMITTED":
                    txIsolation = Isolations.READ_COMMITTED;
                    break;
                case "REPEATABLE READ":
                    txIsolation = Isolations.REPEATABLE_READ;
                    break;
                case "SERIALIZABLE":
                    txIsolation = Isolations.SERIALIZABLE;
                    break;
                default:
                    // can't be happened
                    break;
            }
            service.setTxIsolation(txIsolation);
            service.write(service.getSession2().getOKPacket());
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
        if (charset.toLowerCase().equals("default")) {
            charset = SystemConfig.getInstance().getCharset();
        }
        charset = StringUtil.removeApostropheOrBackQuote(charset.toLowerCase());
        if (checkCharset(charset)) {
            return charset;
        }
        return null;
    }


    private static boolean checkSetNamesSyntax(String stmt) {
        //druid parser can't find syntax error,use regex to check again, but it is not strict
        String regex = "set\\s+names\\s+[`']?[a-zA-Z_0-9]+[`']?(\\s+collate\\s+[`']?[a-zA-Z_0-9]+[`']?)?;?\\s*$";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher ma = pattern.matcher(stmt);
        return ma.find();
    }

    private static NamesInfo checkSetNames(String stmt, String charset, String collate) {
        if (collate == null && !(checkSetNamesSyntax(stmt))) {
            return null;
        }
        if (charset.toLowerCase().equals("default")) {
            charset = SystemConfig.getInstance().getCharset();
        } else {
            charset = StringUtil.removeApostropheOrBackQuote(charset.toLowerCase());
            if (!checkCharset(charset)) {
                return new NamesInfo(null, null);
            }
        }
        if (collate == null) {
            collate = CharsetUtil.getDefaultCollation(charset);
        } else {
            collate = collate.toLowerCase();
            if (collate.equals("default")) {
                collate = CharsetUtil.getDefaultCollation(charset);
            } else {
                int collateIndex = CharsetUtil.getCollationIndexByCharset(charset, collate);
                if (collateIndex == 0) {
                    return new NamesInfo(null, null);
                } else if (collateIndex < 0) {
                    return new NamesInfo(charset, null);
                }
            }
        }
        NamesInfo namesInfo = new NamesInfo(charset, collate);
        if (!CharsetUtil.checkCharsetClient(charset)) {
            namesInfo.isSupport = false;
        }
        return namesInfo;
    }

    private static class NamesInfo {
        private String charset;
        private String collation;
        private boolean isSupport = true;

        NamesInfo(String charset, String collation) {
            this.charset = charset;
            this.collation = collation;
        }
    }
}
