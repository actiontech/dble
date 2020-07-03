/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.Map;

public class SetCallBack implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private ShardingService service;
    private boolean backToOtherThread;

    SetCallBack(ShardingService service) {
        this.service = service;
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            service.executeContextSetTask();
            backToOtherThread = service.executeInnerSetTask();
        } else {
            service.getContextTask().clear();
            service.getInnerSetTask().clear();
        }
    }

    public boolean isBackToOtherThread() {
        return backToOtherThread;
    }
}
