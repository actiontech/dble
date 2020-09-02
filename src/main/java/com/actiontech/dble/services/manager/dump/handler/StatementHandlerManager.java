package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.server.parser.ServerParse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StatementHandlerManager {

    private StatementHandlerManager() {
    }

    private static Map<String, StatementHandler> handlers = new ConcurrentHashMap<>(8);

    static {
        handlers.put("schema", new SchemaHandler());
        handlers.put("table", new TableHandler());
        handlers.put("insert", new InsertHandler());
        handlers.put("default", new DefaultHandler());
    }

    public static StatementHandler getHandler(int sqlType) {
        // parse ddl or create database
        if (ServerParse.CREATE_DATABASE == sqlType || ServerParse.USE == (0xff & sqlType)) {
            return handlers.get("schema");
        }
        if (ServerParse.LOCK == sqlType || ServerParse.DDL == sqlType) {
            return handlers.get("table");
        }
        if (ServerParse.INSERT == sqlType) {
            return handlers.get("insert");
        }
        return handlers.get("default");
    }

}
