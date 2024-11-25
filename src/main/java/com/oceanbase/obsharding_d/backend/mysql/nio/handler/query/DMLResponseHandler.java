/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.plan.util.ComplexQueryPlanUtil;

import java.util.List;

public interface DMLResponseHandler extends ResponseHandler {
    enum HandlerType {
        TEMPTABLE, BASESEL, EASY_MERGE, MERGE_AND_ORDER, FAKE_MERGE, JOIN, NOT_IN, WHERE, GROUPBY, HAVING, ORDERBY, LIMIT, UNION, DISTINCT, SENDMAKER, FINAL, SCALAR_SUB_QUERY, IN_SUB_QUERY, ALL_ANY_SUB_QUERY, RENAME_FIELD, MANAGER_SENDMAKER, UPDATE_QUERY, BASE_UPDATE, EASY_MERGE_UPDATE, MERGE_UPDATE
    }

    enum ExplainType {

        AGGREGATE, DISTINCT, LIMIT, WHERE_FILTER, HAVING_FILTER, SHUFFLE_FIELD, UNION_ALL, ORDER, NOT_IN,
        INNER_FUNC_ADD, JOIN, DIRECT_GROUP, NEST_LOOP, IN_SUB_QUERY, ALL_ANY_SUB_QUERY, SCALAR_SUB_QUERY,
        RENAME_DERIVED_SUB_QUERY, WRITE_TO_CLIENT, HINT_NEST_LOOP,
        TYPE_UPDATE_SUB_QUERY(ComplexQueryPlanUtil.TYPE_UPDATE_SUB_QUERY), MERGE_UPDATE, OTHER;

        private String content;

        ExplainType(String content) {
            this.content = content;
        }

        ExplainType() {
        }

        public String getContent() {
            if (content == null || content.isEmpty()) {
                return super.name();
            } else {
                return content;
            }
        }

    }

    default ExplainType explainType() {
        return ExplainType.OTHER;
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
