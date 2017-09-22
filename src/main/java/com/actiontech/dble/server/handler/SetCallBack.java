/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.Map;

public class SetCallBack implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private ServerConnection sc;

    SetCallBack(ServerConnection sc) {
        this.sc = sc;
    }
    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            sc.executeTask();
        } else {
            sc.getContextTask().clear();
        }
    }
}
