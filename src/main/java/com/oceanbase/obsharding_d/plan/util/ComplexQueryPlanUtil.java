/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.util;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.*;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.util.CollectionUtil;

import java.util.*;

public final class ComplexQueryPlanUtil {

    public static final String TYPE_UPDATE_SUB_QUERY = "for CHILD in UPDATE_SUB_QUERY.RESULTS";

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
        String rootName = buildHandlerTree(endHandler, refMap, new HashMap<>(), nameMap, subQueries);
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
        List<DMLResponseHandler> mergeHandlers = endHandler.getMerges();
        for (DMLResponseHandler startHandler : mergeHandlers) {
            MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
            Set<String> dependenciesSet = mergeHandler.getDependencies();
            if (!dependenciesSet.isEmpty()) {
                dependencies = dependenciesSet;
            } else if (!CollectionUtil.isEmpty(dependencies)) {
                dependencies.removeIf(dependency -> dependency.startsWith(JoinNode.Strategy.HINT_NEST_LOOP.name()));
            }
            boolean isSubUpdate = false;
            if (dependencies != null && dependencies.size() > 0) {
                isSubUpdate = dependencies.stream()
                        .allMatch(entity -> entity.contains(ComplexQueryPlanUtil.TYPE_UPDATE_SUB_QUERY.toLowerCase())) && mergeHandler instanceof MultiNodeUpdateHandler;
            }
            String mergeName = getMergeType(mergeHandler);
            List<BaseDMLHandler> mergeList = new ArrayList<>();
            mergeList.addAll(mergeHandler.getExeHandlers());
            String mergeNode = genHandlerName(mergeName, nameMap);
            ReferenceHandlerInfo refInfo = new ReferenceHandlerInfo(mergeNode, mergeName, mergeHandler, isSubUpdate);
            if (mergeHandler instanceof MultiNodeFakeHandler) {
                refInfo.setBaseSQL(((MultiNodeFakeHandler) mergeHandler).toSQLString());
            }
            handlerMap.put(mergeHandler, refInfo);
            refMap.put(mergeNode, refInfo);
            for (BaseDMLHandler exeHandler : mergeList) {
                RouteResultsetNode rrss = exeHandler.getRrss();
                String dateNode = rrss.getName() + "_" + rrss.getMultiplexNum();
                refInfo.addChild(dateNode);
                String type = "BASE SQL";
                if (dependencies != null && dependencies.size() > 0) {
                    type += "(May No Need)";
                }
                ReferenceHandlerInfo baseSQLInfo = new ReferenceHandlerInfo(dateNode, type, rrss.getStatement(), exeHandler, isSubUpdate);
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
            String handlerType = nextHandler.explainType().getContent();
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
                endHandler.setNextHandlerOnly(nextHandler);
                MultiNodeMergeHandler dmlResponseHandler = (MultiNodeMergeHandler) ((TempTableHandler) handler).getCreatedHandler().getMerges().get(0);
                dmlResponseHandler.getDependencies().add(childName + "'s RESULTS");
                rootName = buildHandlerTree(endHandler, refMap, handlerMap, nameMap, Collections.singleton(childName + "'s RESULTS"));
            }
            if (handler instanceof SendMakeHandler) {
                Set<BaseDMLHandler> tableHandlers = ((SendMakeHandler) handler).getTableHandlers();
                for (BaseDMLHandler tableHandler : tableHandlers) {
                    if (tableHandler instanceof DelayTableHandler) {
                        StringBuilder sb = new StringBuilder(tableHandler.explainType().getContent());
                        sb.append(" - ").append(childName).append("'s RESULTS");
                        MultiNodeMergeHandler dmlResponseHandler = (MultiNodeMergeHandler) ((DelayTableHandler) tableHandler).getCreatedHandler().getMerges().get(0);
                        dmlResponseHandler.getDependencies().add(sb.toString());
                    }
                }
                tableHandlers.clear();
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
        } else if (handler instanceof MultiNodeUpdateHandler) {
            return "MERGE";
        } else {
            return "MERGE_AND_ORDER";
        }
    }

}
