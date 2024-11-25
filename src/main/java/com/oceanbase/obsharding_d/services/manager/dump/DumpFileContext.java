/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ChildTableConfig;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * @author Baofengqi
 */
public final class DumpFileContext {

    // current sharding
    private String schema;
    private List<String> defaultShardingNodes;
    private Set<String> allShardingNodes;

    // current table
    private String table;
    private BaseTableConfig tableConfig;
    private int partitionColumnIndex = -1;
    private int incrementColumnIndex = -1;

    // other
    private boolean isSkip = false;
    private DumpFileWriter writer;
    private List<ErrorMsg> errors;
    private DumpFileConfig config;

    public DumpFileContext() {
    }

    public DumpFileContext(DumpFileWriter writer, DumpFileConfig config) {
        this.writer = writer;
        this.errors = Lists.newCopyOnWriteArrayList();
        this.config = config;
    }

    public DumpFileContext(String schema, List<String> defaultShardingNodes, Set<String> allShardingNodes, DumpFileWriter writer, DumpFileConfig config,
                           String table, BaseTableConfig tableConfig, int partitionColumnIndex, int incrementColumnIndex, boolean isSkip) {
        this.schema = schema;
        this.defaultShardingNodes = defaultShardingNodes;
        this.allShardingNodes = allShardingNodes;
        this.writer = writer;
        this.errors = Lists.newCopyOnWriteArrayList();
        this.config = config;
        this.table = table;
        this.tableConfig = tableConfig;
        this.partitionColumnIndex = partitionColumnIndex;
        this.incrementColumnIndex = incrementColumnIndex;
        this.isSkip = isSkip;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) throws DumpException {
        SchemaConfig schemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(schema);
        if (schemaConfig == null) {
            throw new DumpException("schema[" + schema + "] doesn't exist in config.");
        }
        this.schema = schema;
        this.defaultShardingNodes = schemaConfig.getDefaultShardingNodes();
        this.allShardingNodes = schemaConfig.getAllShardingNodes();
        this.table = null;
    }

    void setDefaultSchema(SchemaConfig schemaConfig) {
        this.schema = schemaConfig.getName();
        this.defaultShardingNodes = schemaConfig.getDefaultShardingNodes();
        this.allShardingNodes = schemaConfig.getAllShardingNodes();
    }

    public boolean isSkipContext() {
        return this.isSkip;
    }

    public void setSkipContext(boolean skip) {
        this.isSkip = skip;
    }


    public List<String> getDefaultShardingNodes() {
        return defaultShardingNodes;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) throws DumpException {
        if (table == null) {
            this.tableConfig = null;
            return;
        }
        if (table.equals(this.table) || table.equalsIgnoreCase(this.table)) {
            return;
        }
        this.table = table;
        this.isSkip = false;
        this.partitionColumnIndex = -1;
        this.incrementColumnIndex = -1;
        if (this.schema == null) {
            throw new DumpException("Can't tell which schema the table[" + table + "] belongs to.");
        }
        this.tableConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(schema).getTables().get(table);
        if (this.tableConfig == null && this.defaultShardingNodes == null) {
            throw new DumpException("schema " + schema + " has no default node.");
        }
        if (this.tableConfig != null && this.tableConfig instanceof ChildTableConfig) {
            throw new DumpException("can't process child table, skip.");
        }

    }

    public BaseTableConfig getTableConfig() {
        return tableConfig;
    }

    public void setTableConfig(BaseTableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    public int getPartitionColumnIndex() {
        return partitionColumnIndex;
    }

    public void setPartitionColumnIndex(int partitionColumnIndex) {
        this.partitionColumnIndex = partitionColumnIndex;
    }

    public int getIncrementColumnIndex() {
        return incrementColumnIndex;
    }

    public void setIncrementColumnIndex(int incrementColumnIndex) {
        this.incrementColumnIndex = incrementColumnIndex;
    }

    public DumpFileWriter getWriter() {
        return writer;
    }

    public void addError(String error) {
        StringBuilder target = new StringBuilder(100);
        if (schema != null) {
            target.append(schema);
        }
        if (schema != null && table != null) {
            target.append('-');
        }
        if (table != null) {
            target.append(table);
        }
        if (schema == null && table == null) {
            target = new StringBuilder("ERROR");
        }
        this.errors.add(new ErrorMsg(target.toString(), error));
    }


    public List<ErrorMsg> getErrors() {
        return errors;
    }

    public DumpFileConfig getConfig() {
        return config;
    }

    public Set<String> getAllShardingNodes() {
        return allShardingNodes;
    }

    public DumpFileContext copyOf(DumpFileContext context) {
        return new DumpFileContext(context.getSchema(), context.getDefaultShardingNodes(), context.getAllShardingNodes(), context.getWriter(), context.getConfig(),
                context.getTable(), context.getTableConfig(), context.getPartitionColumnIndex(), context.getIncrementColumnIndex(), context.isSkip);
    }

}
