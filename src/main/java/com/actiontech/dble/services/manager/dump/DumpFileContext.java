package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Baofengqi
 */
public final class DumpFileContext {

    // current sharding
    private String schema;
    private String defaultShardingNode;
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
    private boolean needSkipError;
    private DumpFileConfig config;

    public DumpFileContext(DumpFileWriter writer, DumpFileConfig config) {
        this.writer = writer;
        this.errors = new ArrayList<>(10);
        this.config = config;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) throws DumpException {
        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
        if (schemaConfig == null) {
            throw new DumpException("schema[" + schema + "] doesn't exist in config.");
        }
        this.schema = schema;
        this.defaultShardingNode = schemaConfig.getShardingNode();
        this.allShardingNodes = schemaConfig.getAllShardingNodes();
        this.table = null;
    }

    void setDefaultSchema(SchemaConfig schemaConfig) {
        this.schema = schemaConfig.getName();
        this.defaultShardingNode = schemaConfig.getShardingNode();
        this.allShardingNodes = schemaConfig.getAllShardingNodes();
    }

    public boolean isSkipContext() {
        return this.isSkip;
    }

    public void setSkipContext(boolean skip) {
        this.isSkip = skip;
    }


    public String getDefaultShardingNode() {
        return defaultShardingNode;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) throws DumpException {
        if (table == null) {
            this.tableConfig = null;
            return;
        }
        if (table.equalsIgnoreCase(this.table)) {
            return;
        }
        this.table = table;
        this.isSkip = false;
        this.partitionColumnIndex = -1;
        this.incrementColumnIndex = -1;
        this.needSkipError = false;
        if (this.schema == null) {
            throw new DumpException("Can't tell which schema the table[" + table + "] belongs to.");
        }
        this.tableConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema).getTables().get(table);
        if (this.tableConfig == null && this.defaultShardingNode == null) {
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
        this.errors.add(new ErrorMsg(schema + "-" + table, error));
    }

    public List<ErrorMsg> getErrors() {
        return errors;
    }

    public boolean isNeedSkipError() {
        return needSkipError;
    }

    public void setNeedSkipError(boolean needSkipError) {
        this.needSkipError = needSkipError;
    }

    public DumpFileConfig getConfig() {
        return config;
    }

    public Set<String> getAllShardingNodes() {
        return allShardingNodes;
    }

}
