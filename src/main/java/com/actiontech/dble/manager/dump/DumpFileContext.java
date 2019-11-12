package com.actiontech.dble.manager.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.TableConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baofengqi
 */
public final class DumpFileContext {

    private String stmt;

    private String schema;
    private String defaultDataNode;
    private String table;
    private TableConfig tableConfig;
    private int partitionColumnIndex = -1;
    private int incrementColumnIndex = -1;

    private boolean isSkip = false;
    private boolean globalCheck = DbleServer.getInstance().getConfig().getSystem().getUseGlobleTableCheck() == 1;
    private DumpFileWriter writer;
    private List<ErrorMsg> errors;
    private boolean needSkipError;
    private DumpFileConfig config;

    public DumpFileContext(DumpFileWriter writer, DumpFileConfig config) {
        this.writer = writer;
        this.errors = new ArrayList<>(10);
        this.config = config;
    }

    public void setStmt(String stmt) {
        this.stmt = stmt;
    }

    public String getStmt() {
        return stmt;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void skipCurrentContext() {
        this.isSkip = true;
    }

    public boolean isSkip() {
        return this.isSkip;
    }

    public boolean isGlobalCheck() {
        return this.globalCheck;
    }

    public String getDefaultDataNode() {
        return defaultDataNode;
    }

    public void setDefaultDataNode(String defaultDataNode) {
        this.defaultDataNode = defaultDataNode;
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
        if (this.tableConfig == null && this.defaultDataNode == null) {
            throw new DumpException("schema " + schema + " has no default node.");
        }
    }

    public boolean isPushDown() {
        return this.tableConfig == null || (!this.tableConfig.isAutoIncrement() && ((tableConfig.isGlobalTable() && !globalCheck) ||
                this.tableConfig.isNoSharding()));
    }

    public TableConfig getTableConfig() {
        return tableConfig;
    }

    public void setTableConfig(TableConfig tableConfig) {
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

}
