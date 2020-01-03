/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;

public enum SessionStage {
    Init,
    Read_SQL, Parse_SQL, Route_Calculation, Prepare_to_Push, Execute_SQL, Fetching_Result,
    First_Node_Fetched_Result, Distributed_Transaction_Commit, Finished, Generate_New_Query,
    Nested_Loop, Easy_Merge, Merge_and_Order, fake_merge, Join, Not_In, Where_Filter, Aggregate, Having_filter,
    Order, Limit, Union, Distinct, Send_Maker, Write_to_Client, Scalar_Sub_Query, In_Sub_Query, All_Any_Sub_Query, Renamed_Filed,;

    public static SessionStage changeFromHandlerType(DMLResponseHandler.HandlerType handlerType) {
        switch (handlerType) {
            case TEMPTABLE:
                return Nested_Loop;
            case BASESEL:
                return Generate_New_Query;
            case EASY_MERGE:
                return Easy_Merge;
            case MERGE_AND_ORDER:
                return Merge_and_Order;
            case FAKE_MERGE:
                return fake_merge;
            case JOIN:
                return Join;
            case NOT_IN:
                return Not_In;
            case WHERE:
                return Where_Filter;
            case GROUPBY:
                return Aggregate;
            case HAVING:
                return Having_filter;
            case ORDERBY:
                return Order;
            case LIMIT:
                return Limit;
            case UNION:
                return Union;
            case DISTINCT:
                return Distinct;
            case SENDMAKER:
                return Send_Maker;
            case FINAL:
                return Write_to_Client;
            case SCALAR_SUB_QUERY:
                return Scalar_Sub_Query;
            case IN_SUB_QUERY:
                return In_Sub_Query;
            case ALL_ANY_SUB_QUERY:
                return All_Any_Sub_Query;
            case RENAME_FIELD:
                return Renamed_Filed;
            default:
                //not happen
        }
        return Write_to_Client; //not happen
    }
}
