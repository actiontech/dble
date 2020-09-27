/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.services.MySQLVariablesService;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.Map;

public class SetCallBack implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private final MySQLVariablesService service;
    private final MysqlVariable[] items;

    SetCallBack(MySQLVariablesService service, MysqlVariable[] items) {
        this.service = service;
        this.items = items;

    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            service.executeContextSetTask(items);
        }
    }

}
