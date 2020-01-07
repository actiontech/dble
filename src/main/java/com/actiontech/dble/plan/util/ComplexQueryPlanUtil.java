/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.*;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.AggregateHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinInnerHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.AllAnySubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.InSubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.SingleRowSubQueryHandler;
import com.actiontech.dble.route.RouteResultsetNode;

import java.util.*;

public final class ComplexQueryPlanUtil {
    private ComplexQueryPlanUtil() {
    }

    public static List<ReferenceHandlerInfo> getComplexQueryResult(BaseHandlerBuilder builder) {
        List<BaseHandlerBuilder> builderList = getBaseHandlerBuilders(builder);
        Map<String, Integer> nameMap = new HashMap<>();
        List<ReferenceHandlerInfo> finalResult = new ArrayList<>();

        Map<BaseHandlerBuilder, String> builderNameMap = new HashMap<>();
        for (int i = builderList.size() - 1; i >= 0; i--) {
            BaseHandlerBuilder tmpBuilder = builderList.get(i);
            Set<String> subQueries = new LinkedHashSet<>();
            for (BaseHandlerBuilder childBuilder : tmpBuilder.getSubQueryBuilderList()) {
                subQueries.add(builderNameMap.get(childBuilder));
            }
            String subQueryRootName = buildResultByEndHandler(subQueries, finalResult, tmpBuilder.getEndHandler(), nameMap);
            builderNameMap.put(tmpBuilder, subQueryRootName);
        }
        return finalResult;
    }

    private static List<BaseHandlerBuilder> getBaseHandlerBuilders(BaseHandlerBuilder builder) {
        Queue<BaseHandlerBuilder> queue = new LinkedList<>();
        queue.add(builder);
        List<BaseHandlerBuilder> builderList = new ArrayList<>();
        while (queue.size() > 0) {
            BaseHandlerBuilder rootBuilder = queue.poll();
            builderList.add(rootBuilder);
            if (rootBuilder.getSubQueryBuilderList().size() > 0) {
                queue.addAll(rootBuilder.getSubQueryBuilderList());
            }
        }
        return builderList;
    }


    private static String buildResultByEndHandler(Set<String> subQueries, List<ReferenceHandlerInfo> finalResult, DMLResponseHandler endHandler, Map<String, Integer> nameMap) {
        Map<String, ReferenceHandlerInfo> refMap = new HashMap<>();
        String rootName = buildHandlerTree(endHandler, refMap, new HashMap<DMLResponseHandler, ReferenceHandlerInfo>(), nameMap, subQueries);
        List<ReferenceHandlerInfo> resultList = new ArrayList<>(refMap.size());
        getDFSHandlers(refMap, rootName, resultList);
        for (int i = resultList.size() - 1; i >= 0; i--) {
            ReferenceHandlerInfo handlerInfo = resultList.get(i);
            finalResult.add(handlerInfo);
        }
        return rootName;
    }

    private static String buildHandlerTree(DMLResponseHandler endHandler, Map<String, ReferenceHandlerInfo> refMap, Map<DMLResponseHandler, ReferenceHandlerInfo> handlerMap, Map<String, Integer> nameMap, Set<String> dependencies) {
        String rootName = null;
        int mergeNodeSize = endHandler.getMerges().size();
        for (int i = 0; i < mergeNodeSize; i++) {
            DMLResponseHandler startHandler = endHandler.getMerges().get(i);
            MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
            String mergeName = getMergeType(mergeHandler);
            List<BaseSelectHandler> mergeList = new ArrayList<>();
            mergeList.addAll(((MultiNodeMergeHandler) startHandler).getExeHandlers());
            String mergeNode = genHandlerName(mergeName, nameMap);
            ReferenceHandlerInfo refInfo = new ReferenceHandlerInfo(mergeNode, mergeName, mergeHandler);
            if (mergeHandler instanceof MultiNodeFakeHandler) {
                refInfo.setBaseSQL(((MultiNodeFakeHandler) mergeHandler).toSQLString());
            }
            handlerMap.put(mergeHandler, refInfo);
            refMap.put(mergeNode, refInfo);
            for (BaseSelectHandler exeHandler : mergeList) {
                RouteResultsetNode rrss = exeHandler.getRrss();
                String dateNode = rrss.getName() + "_" + rrss.getMultiplexNum();
                refInfo.addChild(dateNode);
                String type = "BASE SQL";
                if (dependencies != null && dependencies.size() > 0) {
                    type += "(May No Need)";
                }
                ReferenceHandlerInfo baseSQLInfo = new ReferenceHandlerInfo(dateNode, type, rrss.getStatement(), exeHandler);
                refMap.put(dateNode, baseSQLInfo);
                if (dependencies != null && dependencies.size() > 0) {
                    baseSQLInfo.addAllStepChildren(dependencies);
                }
            }
            String mergeRootName = getAllNodesFromLeaf(mergeHandler, refMap, handlerMap, nameMap);
            if (rootName == null) {
                if (mergeRootName == null) {
                    rootName = mergeNode;
                } else {
                    rootName = mergeRootName;
                }
            }
        }
        return rootName;
    }

    private static void getDFSHandlers(Map<String, ReferenceHandlerInfo> refMap, String rootName, List<ReferenceHandlerInfo> resultList) {
        Stack<ReferenceHandlerInfo> stackSearch = new Stack<>();
        stackSearch.push(refMap.get(rootName));
        while (stackSearch.size() > 0) {
            ReferenceHandlerInfo root = stackSearch.pop();
            resultList.add(root);
            for (String child : root.getChildren()) {
                ReferenceHandlerInfo childRef = refMap.get(child);
                if (childRef != null) {
                    stackSearch.push(childRef);
                }
            }
        }
        refMap.clear();
    }


    private static String genHandlerName(String handlerType, Map<String, Integer> nameMap) {
        String handlerName;
        if (nameMap.containsKey(handlerType)) {
            int number = nameMap.get(handlerType) + 1;
            nameMap.put(handlerType, number);
            handlerName = handlerType.toLowerCase() + "_" + number;
        } else {
            nameMap.put(handlerType, 1);
            handlerName = handlerType.toLowerCase() + "_1";
        }
        return handlerName;
    }

    private static String getAllNodesFromLeaf(DMLResponseHandler handler, Map<String, ReferenceHandlerInfo> refMap, Map<DMLResponseHandler, ReferenceHandlerInfo> handlerMap, Map<String, Integer> nameMap) {
        DMLResponseHandler nextHandler = handler.getNextHandler();
        String rootName = null;
        while (nextHandler != null) {
            ReferenceHandlerInfo child = handlerMap.get(handler);
            String childName = child.getName();
            String handlerType = getTypeName(nextHandler);
            if (!handlerMap.containsKey(nextHandler)) {
                String handlerName = genHandlerName(handlerType, nameMap);
                ReferenceHandlerInfo handlerInfo = new ReferenceHandlerInfo(handlerName, handlerType, nextHandler);
                handlerMap.put(nextHandler, handlerInfo);
                refMap.put(handlerName, handlerInfo);
                handlerInfo.addChild(childName);
                rootName = handlerName;
            } else {
                handlerMap.get(nextHandler).addChild(childName);
            }
            if (handler instanceof TempTableHandler) {
                TempTableHandler tmp = (TempTableHandler) handler;
                DMLResponseHandler endHandler = tmp.getCreatedHandler();
                endHandler.setNextHandler(nextHandler);
                rootName = buildHandlerTree(endHandler, refMap, handlerMap, nameMap, Collections.singleton(childName + "'s RESULTS"));
            }
            handler = nextHandler;
            nextHandler = nextHandler.getNextHandler();
        }
        return rootName;
    }


    private static String getMergeType(DMLResponseHandler handler) {
        if (handler instanceof MultiNodeFakeHandler) {
            return "INNER_FUNC_MERGE";
        } else if (handler instanceof MultiNodeEasyMergeHandler) {
            return "MERGE";
        } else {
            return "MERGE_AND_ORDER";
        }
    }

    private static String getTypeName(DMLResponseHandler handler) {
        if (handler instanceof AggregateHandler) {
            return "AGGREGATE";
        } else if (handler instanceof DistinctHandler) {
            return "DISTINCT";
        } else if (handler instanceof LimitHandler) {
            return "LIMIT";
        } else if (handler instanceof WhereHandler) {
            return "WHERE_FILTER";
        } else if (handler instanceof HavingHandler) {
            return "HAVING_FILTER";
        } else if (handler instanceof SendMakeHandler) {
            return "SHUFFLE_FIELD";
        } else if (handler instanceof UnionHandler) {
            return "UNION_ALL";
        } else if (handler instanceof OrderByHandler) {
            return "ORDER";
        } else if (handler instanceof NotInHandler) {
            return "NOT_IN";
        } else if (handler instanceof JoinInnerHandler) {
            return "INNER_FUNC_ADD";
        } else if (handler instanceof JoinHandler) {
            return "JOIN";
        } else if (handler instanceof DirectGroupByHandler) {
            return "DIRECT_GROUP";
        } else if (handler instanceof TempTableHandler) {
            return "NEST_LOOP";
        } else if (handler instanceof InSubQueryHandler) {
            return "IN_SUB_QUERY";
        } else if (handler instanceof AllAnySubQueryHandler) {
            return "ALL_ANY_SUB_QUERY";
        } else if (handler instanceof SingleRowSubQueryHandler) {
            return "SCALAR_SUB_QUERY";
        } else if (handler instanceof RenameFieldHandler) {
            return "RENAME_DERIVED_SUB_QUERY";
        } else if (handler instanceof OutputHandler) {
            return "WRITE_TO_CLIENT";
        }
        return "OTHER";
    }

}
