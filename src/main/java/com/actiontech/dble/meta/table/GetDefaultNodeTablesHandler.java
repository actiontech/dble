/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.sharding.SchemaConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class GetDefaultNodeTablesHandler extends GetNodeTablesHandler {

    private final Set<String> tables = new LinkedHashSet<>();
    private final Set<String> views = new HashSet<>();
    private SchemaConfig config;

    GetDefaultNodeTablesHandler(SchemaConfig config) {
        super(config.getShardingNode(), !config.isNoSharding());
        this.config = config;
    }

    @Override
    protected void handleTable(String table, String tableType) {
        if (tableType.equalsIgnoreCase("view")) {
            views.add(table);
        } else if (!config.getTables().containsKey(table)) {
            tables.add(table);
        }
    }

    public Set<String> getTables() {
        lock.lock();
        try {
            while (!isFinished) {
                notify.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("getTables() is interrupted.");
            return Collections.emptySet();
        } finally {
            lock.unlock();
        }

        if (!views.isEmpty()) {
            tables.addAll(views);
        }
        return tables;
    }

}
