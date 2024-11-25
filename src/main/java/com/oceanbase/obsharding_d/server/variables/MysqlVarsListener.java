/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.variables;

import com.oceanbase.obsharding_d.sqlengine.SQLQueryResult;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MysqlVarsListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlVarsListener.class);
    private final VarsExtractorHandler handler;

    public MysqlVarsListener(VarsExtractorHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (!result.isSuccess()) {
            //not thread safe
            LOGGER.warn("Can't get variables from shardingNode: " + result.getShardingNode() + "!");
            handler.signalDone(false);
            return;
        }

        Map<String, String> kvs = result.getResult();
        handler.handleVars(kvs);
    }
}
