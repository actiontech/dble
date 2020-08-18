package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/3/24.
 */
public final class SetInnerHandler {


    private SetInnerHandler() {

    }

    public static boolean handleSingleXA(ServerConnection c, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleSingleXA(c, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            c.getSession2().getTransactionManager().setXaTxEnabled(Boolean.valueOf(key), c);
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.write(c.writeToBuffer(c.getSession2().getOkByteArray(), c.allocate()));
            c.getSession2().multiStatementNextSql(multiStatementFlag);
            return true;
        }
        return false;
    }

    public static boolean preHandleSingleXA(ServerConnection c, SQLExpr valueExpr, List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'XA'");
            return false;
        } else if (switchStatus) {
            if (c.getSession2().getTargetMap().size() > 0 && c.getSession2().getSessionXaID() == null) {
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "you can't set xa cmd on when there are unfinished operation in the session.");
                return false;
            }
            innerSetTask.add(new Pair<>(SetHandler.KeyType.XA, new Pair<String, String>("true", null)));
            return true;
        } else {
            if (c.getSession2().getTargetMap().size() > 0 && c.getSession2().getSessionXaID() != null) {
                c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "you can't set xa cmd off when a transaction is in progress.");
                return false;
            }
            innerSetTask.add(new Pair<>(SetHandler.KeyType.XA, new Pair<String, String>("false", null)));
            return true;
        }
    }


    public static boolean handleSingleTrace(ServerConnection c, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleSingleTrace(c, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            c.getSession2().setTrace(Boolean.valueOf(key));
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.write(c.writeToBuffer(c.getSession2().getOkByteArray(), c.allocate()));
            c.getSession2().multiStatementNextSql(multiStatementFlag);
            return true;
        }
        return false;
    }


    public static boolean preHandleSingleTrace(ServerConnection c, SQLExpr valueExpr, List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'TRACE'");
            return false;
        } else {
            innerSetTask.add(new Pair<>(SetHandler.KeyType.TRACE, new Pair<String, String>("" + switchStatus, null)));
            return true;
        }
    }


    public static boolean handleSingleAutocommit(String stmt, ServerConnection c, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleAutocommit(c, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            if (!execSetAutoCommit(stmt, c, Boolean.valueOf(key))) {
                boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
                c.write(c.writeToBuffer(c.getSession2().getOkByteArray(), c.allocate()));
                c.getSession2().multiStatementNextSql(multiStatementFlag);
            }
            return true;
        }
        return false;
    }


    public static boolean preHandleAutocommit(ServerConnection c, SQLExpr valueExpr, List<Pair<SetHandler.KeyType,
            Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'AUTOCOMMIT'");
            return false;
        } else {
            innerSetTask.add(new Pair<>(SetHandler.KeyType.AUTOCOMMIT, new Pair<String, String>("" + switchStatus, null)));
            return true;
        }
    }

    public static boolean execSetAutoCommit(String stmt, ServerConnection c, boolean setValue) {
        if (setValue) {
            if (!c.isAutocommit() && c.getSession2().getTargetCount() > 0) {
                c.getSession2().implicitCommit(() -> {
                    boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
                    c.setAutocommit(true);
                    c.write(c.writeToBuffer(c.getSession2().getOkByteArray(), c.allocate()));
                    c.getSession2().multiStatementNextSql(multiStatementFlag);
                });
                return true;
            }
            c.setAutocommit(true);
        } else {
            if (c.isAutocommit()) {
                c.setAutocommit(false);
                TxnLogHelper.putTxnLog(c, stmt);
            }
            return false;
        }
        return false;
    }


    public static Boolean isSwitchOn(SQLExpr valueExpr) {
        if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            int iValue = value.getNumber().intValue();
            if (iValue < 0 || iValue > 1) {
                return null;
            }
            return (iValue == 1);
        } else if (valueExpr instanceof SQLBooleanExpr) {
            SQLBooleanExpr value = (SQLBooleanExpr) valueExpr;
            return value.getValue();
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

    public static String parseStringValue(SQLExpr valueExpr) {
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
        } else if (valueExpr instanceof SQLDefaultExpr || valueExpr instanceof SQLNullExpr) {
            strValue = valueExpr.toString();
        }
        return strValue;
    }
}
