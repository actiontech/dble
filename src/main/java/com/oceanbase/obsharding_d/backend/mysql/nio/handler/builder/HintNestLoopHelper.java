/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.DelayTableHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.SendMakeHandler;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.route.parser.util.Pair;

import java.util.*;

public class HintNestLoopHelper {

    Map<PlanNode, List<DelayTableHandler>> delayTableHandlerMap = new HashMap<>();
    Map<PlanNode, SendMakeHandler> sendMakeHandlerHashMap = new HashMap<>();
    //hint plan that the node is a dependent node, when in fact the node is not
    Set<PlanNode> fakeDependSet = new HashSet<>();
    Map<PlanNode, Pair<Item, Item>> itemMap = new HashMap<>();

    public Map<PlanNode, SendMakeHandler> getSendMakeHandlerHashMap() {
        return sendMakeHandlerHashMap;
    }

    public Map<PlanNode, List<DelayTableHandler>> getDelayTableHandlerMap() {
        return delayTableHandlerMap;
    }

    public List<DelayTableHandler> getDelayTableHandlers(PlanNode node) {
        List<DelayTableHandler> delayTableHandlerList = Optional.ofNullable(delayTableHandlerMap.get(node)).orElse(new ArrayList<>());
        if (delayTableHandlerList.isEmpty()) {
            delayTableHandlerMap.put(node, delayTableHandlerList);
        }
        return delayTableHandlerList;
    }

    public Map<PlanNode, Pair<Item, Item>> getItemMap() {
        return itemMap;
    }

    public Set<PlanNode> getFakeDependSet() {
        return fakeDependSet;
    }
}
