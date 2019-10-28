package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.handler.dump.type.DumpContent;
import com.actiontech.dble.manager.handler.dump.type.DumpSchema;
import com.actiontech.dble.manager.handler.dump.type.DumpTable;
import com.actiontech.dble.server.util.GlobalTableUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DumpTableHandlerManager {

    private static Map<String, DumpHandler> handlers = new ConcurrentHashMap<>(5);

    static {
        handlers.put("schema", new DumpSchemaHandler());
        handlers.put("global", new GlobalTableHandler());
        handlers.put("sharding", new ShardingTableHandler());
    }

    private DumpTableHandlerManager() {
    }

    public static DumpHandler getHandler(DumpContent content) {
        if (content instanceof DumpSchema) {
            return handlers.get("schema");
        }
        if (content instanceof DumpTable) {
            DumpTable table = (DumpTable) content;
            TableConfig tableConfig = ((DumpTable) content).getTableConfig();
            if (tableConfig == null || tableConfig.isNoSharding() && !tableConfig.isAutoIncrement() ||
                    tableConfig.isGlobalTable() && !GlobalTableUtil.useGlobalTableCheck()) {
                return null;
            }
            if (tableConfig.isGlobalTable()) {
                return handlers.get("global");
            } else if (!table.getTableConfig().isNoSharding()) {
                return handlers.get("sharding");
            }
        }
        return null;
    }
}
