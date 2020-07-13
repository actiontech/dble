/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;

import java.util.List;

public interface DMLResponseHandler extends ResponseHandler {
    enum HandlerType {
        TEMPTABLE, BASESEL, EASY_MERGE, MERGE_AND_ORDER, FAKE_MERGE, JOIN, NOT_IN, WHERE, GROUPBY, HAVING, ORDERBY, LIMIT, UNION, DISTINCT, SENDMAKER, FINAL, SCALAR_SUB_QUERY, IN_SUB_QUERY, ALL_ANY_SUB_QUERY, RENAME_FIELD, MANAGER_SENDMAKER
    }

    HandlerType type();

    DMLResponseHandler getNextHandler();

    void setNextHandler(DMLResponseHandler next);

    void setNextHandlerOnly(DMLResponseHandler next);

    List<DMLResponseHandler> getMerges();

    boolean isAllPushDown();

    void setAllPushDown(boolean allPushDown);

    void setLeft(boolean left);

    void terminate();

}
