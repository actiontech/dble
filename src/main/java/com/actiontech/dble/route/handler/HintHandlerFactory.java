/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import java.util.HashMap;
import java.util.Map;

public final class HintHandlerFactory {

    private static volatile boolean isInit = false;

    private static Map<String, HintHandler> dbleHintHandlerMap = new HashMap<>();
    private static Map<String, HintHandler> uproxyHintHandlerMap = new HashMap<>();

    private HintHandlerFactory() {
    }

    private static void init() {
        dbleHintHandlerMap.put("sql", new HintSQLHandler());
        dbleHintHandlerMap.put("shardingnode", new HintShardingNodeHandler());
        // force master or force slave
        dbleHintHandlerMap.put("db_type", new HintMasterDBHandler());
        dbleHintHandlerMap.put("db_instance_url", new HintDbInstanceHandler());

        uproxyHintHandlerMap.put("master", new HintMasterDBHandler());
        uproxyHintHandlerMap.put("uproxy_dest", new HintDbInstanceHandler());
        isInit = true;
    }

    public static HintHandler getDbleHintHandler(String hintType) {
        if (!isInit) {
            synchronized (HintHandlerFactory.class) {
                if (!isInit) {
                    init();
                }
            }
        }
        return dbleHintHandlerMap.get(hintType);
    }


    public static HintHandler getUproxyHintHandler(String hintType) {
        if (!isInit) {
            synchronized (HintHandlerFactory.class) {
                if (!isInit) {
                    init();
                }
            }
        }
        return uproxyHintHandlerMap.get(hintType);
    }

}
