package io.mycat.route.handler;

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
