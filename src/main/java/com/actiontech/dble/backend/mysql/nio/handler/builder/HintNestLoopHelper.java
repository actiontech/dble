package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.impl.DelayTableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.SendMakeHandler;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.route.parser.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HintNestLoopHelper {

    Map<PlanNode, List<DelayTableHandler>> delayTableHandlerMap = new HashMap<>();
    Map<PlanNode, SendMakeHandler> sendMakeHandlerHashMap = new HashMap<>();
    Map<PlanNode, Pair<Item, Item>> itemMap = new HashMap<>();

    public Map<PlanNode, SendMakeHandler> getSendMakeHandlerHashMap() {
        return sendMakeHandlerHashMap;
    }

    public Map<PlanNode, List<DelayTableHandler>> getDelayTableHandlerMap() {
        return delayTableHandlerMap;
    }

    public Map<PlanNode, Pair<Item, Item>> getItemMap() {
        return itemMap;
    }
}
