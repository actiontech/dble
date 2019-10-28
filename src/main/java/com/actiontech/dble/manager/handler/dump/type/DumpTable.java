package com.actiontech.dble.manager.handler.dump.type;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.handler.dump.Test;
import com.actiontech.dble.util.StringUtil;

import java.util.ArrayList;
import java.util.Map;

public class DumpTable extends DumpContent {

    private String schema;
    private String tableName;
    private TableConfig tableConfig;
    private int partitionColumnIndex = -1;
    private int incrementColumnIndex = -1;
    private Map<String, StringBuilder> inserts;
    private int insertStmtIndex = -1;

    public DumpTable(String schema, String tableName) {
        this.tableName = tableName;
        this.schema = schema;
        this.tableConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema).getTables().get(tableName);
        if (tableConfig != null) {
            this.dataNodes = this.tableConfig.getDataNodes();
        } else if (!StringUtil.isEmpty(Test.getSchemas().get(schema).getDataNode())) {
            this.dataNodes = new ArrayList<>(1);
            this.dataNodes.add(Test.getSchemas().get(schema).getDataNode());
        }
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

    public String getTableName() {
        return tableName;
    }

    public TableConfig getTableConfig() {
        return tableConfig;
    }

    public String getSchema() {
        return schema;
    }

    public void setInserts(Map<String, StringBuilder> inserts) {
        this.inserts = inserts;
    }

    public void replace(String str, boolean isInsert) {
        super.replace(str);
        if (isInsert) {
            insertStmtIndex = currentIndex;
        }
    }

    @Override
    public String get(String dataNode) {
        if (insertStmtIndex != -1 && this.currentIndex == insertStmtIndex && inserts != null) {
            String stmt = inserts.get(dataNode) == null ? "" : inserts.get(dataNode).toString();
            if (stmt.endsWith(",")) {
                stmt = stmt.substring(0, stmt.length() - 1);
            }
            return "\n" + stmt;
        }
        return super.get(dataNode);
    }

    @Override
    public String toString() {
        return "schema[" + schema + "],table[" + tableName + "] ";
    }

}
