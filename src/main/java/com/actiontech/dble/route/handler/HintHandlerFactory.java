/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import java.util.HashMap;
import java.util.Map;

public final class HintHandlerFactory {

    private static volatile boolean isInit = false;

    private static Map<String, HintHandler> hintHandlerMap = new HashMap<>();

    private HintHandlerFactory() {
    }

    private static void init() {
        hintHandlerMap.put("sql", new HintSQLHandler());
        hintHandlerMap.put("schema", new HintSchemaHandler());
        hintHandlerMap.put("datanode", new HintDataNodeHandler());
        // force master or force slave
        hintHandlerMap.put("db_type", new HintMasterDBHandler());
        isInit = true;
    }

    public static HintHandler getHintHandler(String hintType) {
        if (!isInit) {
            synchronized (HintHandlerFactory.class) {
                if (!isInit) {
                    init();
                }
            }
        }
        return hintHandlerMap.get(hintType);
    }

}
