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
import com.actiontech.dble.services.MySQLVariablesService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SetTestJob;
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

    public static void handle(String stmt, MySQLVariablesService frontService, int offset) {
        if (!ParseUtil.isSpace(stmt.charAt(offset))) {
            frontService.writeErrMessage(ErrorCode.ERR_WRONG_USED, stmt + " is not supported");
            return;
        }

        stmt = convertCharsetKeyWord(stmt);

        try {
            SetItem[] items;
            StringBuilder setSQL = new StringBuilder("set ");
            StringBuilder selectSQL = new StringBuilder("select ");
            int userVariableSize = 0;
            // parse set sql
            SQLStatement statement = parseSQL(stmt);
            if (statement instanceof SQLSetStatement) {
                List<SQLAssignItem> assignItems = ((SQLSetStatement) statement).getItems();
                String key;
                int systemVariableIndex = assignItems.size() - 1;
                items = new SetItem[assignItems.size()];

                for (SQLAssignItem sqlAssignItem : assignItems) {
                    // new set item
                    key = handleSetKey(sqlAssignItem.getTarget());
                    SetItem item = newSetItem(key, sqlAssignItem.getValue());
                    if (item.getType() == KeyType.USER_VARIABLES) {
                        if (userVariableSize != 0) {
                            setSQL.append(",");
                            selectSQL.append(",");
                        }
                        setSQL.append(sqlAssignItem.toString());
                        selectSQL.append(item.getName());

                        items[userVariableSize++] = item;
                    } else if (item.getType() == KeyType.SYSTEM_VARIABLES) {
                        if (userVariableSize != 0) {
                            setSQL.append(",");
                        }
                        setSQL.append(sqlAssignItem.toString());
                        items[systemVariableIndex--] = item;
                    } else if (item.getType() == KeyType.XA) {
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
                items = new SetItem[1];
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

    private static SetItem handleTransaction(MySqlSetTransactionStatement setStatement) throws SQLSyntaxErrorException {
        //always single
        SetItem item;
        if (setStatement.getGlobal() == null) {
            throw new SQLSyntaxErrorException("setting transaction without any SESSION or GLOBAL keyword is not supported now");
        } else if (setStatement.getGlobal()) {
            throw new SQLSyntaxErrorException("setting GLOBAL value is not supported");
        } else if (setStatement.getAccessModel() != null) {
            if (setStatement.getAccessModel().equals("ONLY")) {
                item = newSetItem(VersionUtil.TX_READ_ONLY, new SQLBooleanExpr(true));
            } else {
                item = newSetItem(VersionUtil.TX_READ_ONLY, new SQLBooleanExpr(false));
            }
        } else {
            item = newSetItem(VersionUtil.TRANSACTION_ISOLATION, new SQLCharExpr(setStatement.getIsolationLevel()));
        }
        return item;
    }

    //execute multiStmt and callback to reset conn
    private static void checkVariables(MySQLVariablesService service, String setSql, SetItem[] items, int userVariableSize) {
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

    private static SetItem newSetItem(String key, SQLExpr valueExpr) throws SQLSyntaxErrorException {
        switch (key.toLowerCase()) {
            case "xa":
                return new SetItem("xa", SetItemUtil.getBooleanVal(valueExpr), SetHandler.KeyType.XA);
            case "trace":
                return new SetItem("trace", SetItemUtil.getBooleanVal(valueExpr), SetHandler.KeyType.TRACE);
            case "autocommit":
                return new SetItem("autocommit", SetItemUtil.getBooleanVal(valueExpr), SetHandler.KeyType.AUTOCOMMIT);
            case "collation_connection":
                return new SetItem("collation_connection", SetItemUtil.getCollationVal(valueExpr), SetHandler.KeyType.COLLATION_CONNECTION);
            case "character_set_client":
                return new SetItem("character_set_client", SetItemUtil.getCharsetClientVal(valueExpr), SetHandler.KeyType.CHARACTER_SET_CLIENT);
            case "character_set_results":
                return new SetItem("character_set_results", SetItemUtil.getCharsetResultsVal(valueExpr), SetHandler.KeyType.CHARACTER_SET_RESULTS);
            case "character_set_connection":
                return new SetItem("character_set_connection", SetItemUtil.getCharsetConnectionVal(valueExpr), SetHandler.KeyType.CHARACTER_SET_CONNECTION);
            case "character set":
                return new SetItem(key, SetItemUtil.getCharsetVal(valueExpr), SetHandler.KeyType.CHARSET);
            case "names":
                return new SetItem(key, SetItemUtil.getNamesVal(valueExpr), SetHandler.KeyType.NAMES);
            case VersionUtil.TRANSACTION_ISOLATION:
            case VersionUtil.TX_ISOLATION:
                return new SetItem(key, SetItemUtil.getIsolationVal(valueExpr), SetHandler.KeyType.TX_ISOLATION);
            case VersionUtil.TRANSACTION_READ_ONLY:
            case VersionUtil.TX_READ_ONLY:
                return new SetItem(key, SetItemUtil.getBooleanVal(valueExpr), SetHandler.KeyType.TX_READ_ONLY);
            default:
                if (key.startsWith("@@")) {
                    return new SetItem(key.substring(2), SetItemUtil.parseVariablesValue(valueExpr), KeyType.SYSTEM_VARIABLES);
                } else if (key.startsWith("@")) {
                    return new SetItem(key.toUpperCase(), null, KeyType.USER_VARIABLES);
                }
                return new SetItem(key, SetItemUtil.parseVariablesValue(valueExpr), KeyType.SYSTEM_VARIABLES);
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

    public static class SetItem {
        private String name;
        private String value;
        private SetHandler.KeyType type;

        public SetItem(String name, String value, SetHandler.KeyType type) {
            this.name = name;
            this.value = value;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public KeyType getType() {
            return type;
        }

        public void setType(KeyType type) {
            this.type = type;
        }
    }

}
