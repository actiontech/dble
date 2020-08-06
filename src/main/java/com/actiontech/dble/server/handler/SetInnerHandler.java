package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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

    public static boolean handleSingleXA(ShardingService shardingService, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleSingleXA(shardingService, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            shardingService.getSession2().getTransactionManager().setXaTxEnabled(Boolean.valueOf(key), shardingService);
            shardingService.write(shardingService.getSession2().getOKPacket());
            return true;
        }
        return false;
    }

    public static boolean preHandleSingleXA(ShardingService shardingService, SQLExpr valueExpr, List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            shardingService.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'XA'");
            return false;
        } else if (switchStatus) {
            if (shardingService.getSession2().getTargetMap().size() > 0 && shardingService.getSession2().getSessionXaID() == null) {
                shardingService.writeErrMessage(ErrorCode.ERR_WRONG_USED, "you can't set xa cmd on when there are unfinished operation in the session.");
                return false;
            }
            innerSetTask.add(new Pair<>(SetHandler.KeyType.XA, new Pair<String, String>("true", null)));
            return true;
        } else {
            if (shardingService.getSession2().getTargetMap().size() > 0 && shardingService.getSession2().getSessionXaID() != null) {
                shardingService.writeErrMessage(ErrorCode.ERR_WRONG_USED, "you can't set xa cmd off when a transaction is in progress.");
                return false;
            }
            innerSetTask.add(new Pair<>(SetHandler.KeyType.XA, new Pair<String, String>("false", null)));
            return true;
        }
    }


    public static boolean handleSingleTrace(ShardingService shardingService, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleSingleTrace(shardingService, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            shardingService.getSession2().setTrace(Boolean.valueOf(key));
            shardingService.write(shardingService.getSession2().getOKPacket());
            return true;
        }
        return false;
    }


    public static boolean preHandleSingleTrace(ShardingService service, SQLExpr valueExpr, List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            service.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'TRACE'");
            return false;
        } else {
            innerSetTask.add(new Pair<>(SetHandler.KeyType.TRACE, new Pair<String, String>("" + switchStatus, null)));
            return true;
        }
    }


    public static boolean handleSingleAutocommit(String stmt, ShardingService service, SQLExpr valueExpr) {
        List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();
        if (preHandleAutocommit(service, valueExpr, innerSetTask)) {
            String key = innerSetTask.get(0).getValue().getKey();
            if (!execSetAutoCommit(stmt, service, Boolean.valueOf(key))) {
                service.write(service.getSession2().getOKPacket());
            }
            return true;
        }
        return false;
    }


    public static boolean preHandleAutocommit(ShardingService service, SQLExpr valueExpr, List<Pair<SetHandler.KeyType,
            Pair<String, String>>> innerSetTask) {
        Boolean switchStatus = isSwitchOn(valueExpr);
        if (switchStatus == null) {
            service.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR, "Incorrect argument type to variable 'AUTOCOMMIT'");
            return false;
        } else {
            innerSetTask.add(new Pair<>(SetHandler.KeyType.AUTOCOMMIT, new Pair<String, String>("" + switchStatus, null)));
            return true;
        }
    }

    public static boolean execSetAutoCommit(String stmt, ShardingService shardingService, boolean setValue) {
        if (setValue) {
            if (!shardingService.isAutocommit() && shardingService.getSession2().getTargetCount() > 0) {
                shardingService.getSession2().implicitCommit(() -> {
                    shardingService.write(shardingService.getSession2().getOKPacket());
                });
                shardingService.setAutocommit(true);
                return true;
            }
            shardingService.setAutocommit(true);
        } else {
            if (shardingService.isAutocommit()) {
                shardingService.setAutocommit(false);
                TxnLogHelper.putTxnLog(shardingService, stmt);
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
