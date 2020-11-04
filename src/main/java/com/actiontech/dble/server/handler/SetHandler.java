/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.util.SetItemUtil;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SetTestJob;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLSyntaxErrorException;
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

    public static void handle(String stmt, BusinessService frontService, int offset) {
        if (!ParseUtil.isSpace(stmt.charAt(offset))) {
            frontService.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
            return;
        }

        stmt = convertCharsetKeyWord(stmt);

        try {
            MysqlVariable[] items;
            StringBuilder setSQL = new StringBuilder("set ");
            StringBuilder selectSQL = new StringBuilder("select ");
            int userVariableSize = 0;
            // parse set sql
            SQLStatement statement = parseSQL(stmt);
            if (statement instanceof SQLSetStatement) {
                List<SQLAssignItem> assignItems = ((SQLSetStatement) statement).getItems();
                String key;
                int systemVariableIndex = assignItems.size() - 1;
                items = new MysqlVariable[assignItems.size()];

                for (SQLAssignItem sqlAssignItem : assignItems) {
                    // new set item
                    key = handleSetKey(sqlAssignItem.getTarget());
                    MysqlVariable item = newSetItem(key, sqlAssignItem.getValue());
                    if (item.getType() == VariableType.USER_VARIABLES) {
                        if (setSQL.length() > 4) {
                            setSQL.append(",");
                        }
                        if (selectSQL.length() > 7) {
                            selectSQL.append(",");
                        }
                        setSQL.append(SQLUtils.toMySqlString(sqlAssignItem));
                        selectSQL.append(item.getName());

                        items[userVariableSize++] = item;
                    } else if (item.getType() == VariableType.SYSTEM_VARIABLES) {
                        if (setSQL.length() > 4) {
                            setSQL.append(",");
                        }
                        setSQL.append(SQLUtils.toMySqlString(sqlAssignItem));
                        items[systemVariableIndex--] = item;
                    } else if (item.getType() == VariableType.XA) {
                        if (frontService instanceof ShardingService) {
                            boolean val = Boolean.parseBoolean(item.getValue());
                            ((ShardingService) frontService).checkXaStatus(val);
                            items[systemVariableIndex--] = item;
                        } else {
                            throw new SQLSyntaxErrorException("unsupported set xa");
                        }
                    } else {
                        items[systemVariableIndex--] = item;
                    }
                }
            } else if (statement instanceof MySqlSetTransactionStatement) {
                items = new MysqlVariable[1];
                items[0] = handleTransaction((MySqlSetTransactionStatement) statement);
            } else {
                frontService.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
                return;
            }

            // check user variables and system variables unused in dble
            if (setSQL.length() > 4) {
                if (userVariableSize > 0) {
                    setSQL.append(";").append(selectSQL);
                }
                checkVariables(frontService, setSQL.toString(), items, userVariableSize);
            } else {
                frontService.executeContextSetTask(items);
            }

        } catch (SQLSyntaxErrorException e) {
            frontService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }

    private static MysqlVariable handleTransaction(MySqlSetTransactionStatement setStatement) throws SQLSyntaxErrorException {
        //always single
        MysqlVariable item;
        if (setStatement.getGlobal() == null) {
            throw new SQLSyntaxErrorException("setting transaction without any SESSION or GLOBAL keyword is not supported now");
        } else if (setStatement.getGlobal()) {
            throw new SQLSyntaxErrorException("setting GLOBAL value is not supported");
        } else if (setStatement.getAccessModel() != null) {
            if (setStatement.getAccessModel().equals("ONLY")) {
                item = newSetItem(VersionUtil.TX_READ_ONLY, new SQLIntegerExpr(1));
            } else {
                item = newSetItem(VersionUtil.TX_READ_ONLY, new SQLIntegerExpr(0));
            }
        } else {
            item = newSetItem(VersionUtil.TRANSACTION_ISOLATION, new SQLCharExpr(setStatement.getIsolationLevel()));
        }
        return item;
    }

    //execute multiStmt and callback to reset conn
    private static void checkVariables(BusinessService service, String setSql, MysqlVariable[] items, int userVariableSize) {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SetCallBack(service, items));
        SetTestJob sqlJob = new SetTestJob(setSql, resultHandler, items, userVariableSize, service);
        sqlJob.run();
    }

    private static String handleSetKey(SQLExpr key) throws SQLSyntaxErrorException {
        if (key instanceof SQLPropertyExpr) {
            SQLPropertyExpr target = (SQLPropertyExpr) key;
            if (!(target.getOwner() instanceof SQLVariantRefExpr)) {
                throw new SQLSyntaxErrorException("unsupport global");
            }
            SQLVariantRefExpr owner = (SQLVariantRefExpr) target.getOwner();
            if (owner.isGlobal()) {
                throw new SQLSyntaxErrorException("unsupport global");
            }
            return target.getName();
        } else if (key instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr target = (SQLVariantRefExpr) key;
            if (target.isGlobal()) {
                throw new SQLSyntaxErrorException("unsupport global");
            }
            return target.getName();
        } else if (key instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr target = (SQLIdentifierExpr) key;
            return target.getLowerName();
        }
        throw new SQLSyntaxErrorException("unknown key type");
    }

    private static MysqlVariable newSetItem(String key, SQLExpr valueExpr) throws SQLSyntaxErrorException {
        switch (key.toLowerCase()) {
            case "xa":
                return new MysqlVariable("xa", SetItemUtil.getBooleanVal(valueExpr), VariableType.XA);
            case "trace":
                return new MysqlVariable("trace", SetItemUtil.getBooleanVal(valueExpr), VariableType.TRACE);
            case "autocommit":
                return new MysqlVariable("autocommit", SetItemUtil.getBooleanVal(valueExpr), VariableType.AUTOCOMMIT);
            case "collation_connection":
                return new MysqlVariable("collation_connection", SetItemUtil.getCollationVal(valueExpr), VariableType.COLLATION_CONNECTION);
            case "character_set_client":
                return new MysqlVariable("character_set_client", SetItemUtil.getCharsetClientVal(valueExpr), VariableType.CHARACTER_SET_CLIENT);
            case "character_set_results":
                return new MysqlVariable("character_set_results", SetItemUtil.getCharsetResultsVal(valueExpr), VariableType.CHARACTER_SET_RESULTS);
            case "character_set_connection":
                return new MysqlVariable("character_set_connection", SetItemUtil.getCharsetConnectionVal(valueExpr), VariableType.CHARACTER_SET_CONNECTION);
            case "character set":
                return new MysqlVariable(key, SetItemUtil.getCharsetVal(valueExpr), VariableType.CHARSET);
            case "names":
                return new MysqlVariable(key, SetItemUtil.getNamesVal(valueExpr), VariableType.NAMES);
            case VersionUtil.TRANSACTION_ISOLATION:
            case VersionUtil.TX_ISOLATION:
                return new MysqlVariable(key, SetItemUtil.getIsolationVal(valueExpr), VariableType.TX_ISOLATION);
            case VersionUtil.TRANSACTION_READ_ONLY:
            case VersionUtil.TX_READ_ONLY:
                return new MysqlVariable(key, SetItemUtil.getBooleanVal(valueExpr), VariableType.TX_READ_ONLY);
            default:
                if (key.startsWith("@@")) {
                    return newSetItem(key.substring(2), valueExpr);
                } else if (key.startsWith("@")) {
                    return new MysqlVariable(key.toUpperCase(), null, VariableType.USER_VARIABLES);
                }
                return new MysqlVariable(key, SetItemUtil.parseVariablesValue(valueExpr), VariableType.SYSTEM_VARIABLES);
        }
    }

    private static SQLStatement parseSQL(String stmt) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        try {
            return parser.parseStatement();
        } catch (Exception t) {
            throw new SQLSyntaxErrorException(t);
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
                result.append(stmt, tailStart, index);
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

}
