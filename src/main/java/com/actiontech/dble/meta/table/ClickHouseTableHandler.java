package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.ApNode;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClickHouseTableHandler extends ModeTableHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTableHandler.class);

    private final AbstractSchemaMetaHandler operationalHandler;
    protected final String schema;
    protected final SchemaConfig schemaConfig;
    private final ProxyMetaManager tmManager;
    protected boolean isFinished = false;
    protected Lock lock = new ReentrantLock();
    protected Condition notify = lock.newCondition();

    public ClickHouseTableHandler(AbstractSchemaMetaHandler operationalHandler, ProxyMetaManager tmManager) {
        this.operationalHandler = operationalHandler;
        this.schemaConfig = operationalHandler.getSchemaConfig();
        this.schema = operationalHandler.getSchema();
        this.tmManager = tmManager;
    }

    @Override
    boolean loadMetaData() {
        Set<String> tables = new HashSet<>();
        SchemaMeta schemaMeta = tmManager.getCatalogs().get(schema);
        if (schemaMeta != null) {
            schemaMeta.getTableMetas().values().stream().forEach(f -> tables.add(f.getTableName()));
        }
        schemaConfig.getTables().values().stream().forEach(f -> tables.add(f.getName()));

        if (!CollectionUtil.isEmpty(tables)) {
            ShowTableByNodeUnitHandler unitHandler = new ShowTableByNodeUnitHandler(tables, schemaConfig.getDefaultApNode());
            unitHandler.execute();
            Set<String> noExistTable = unitHandler.getNotExistTable();
            if (!CollectionUtil.isEmpty(noExistTable)) {
                for (String tableName : noExistTable) {
                    String tableLackKey = AlertUtil.getTableLackKey2(schemaConfig.getDefaultApNode(), tableName);
                    String warnMsg = "Can't get table " + tableName + "'s config from apNode:" + schemaConfig.getDefaultApNode() + "! Maybe the table is not initialized!";
                    LOGGER.warn(warnMsg);
                    AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableLackKey));
                    ToResolveContainer.TABLE_LACK.add(tableLackKey);
                }
            }
        }
        return true;
    }

    @Override
    void handleTable(String table, String apNode, boolean isView, String sql) {
    }

    @Override
    void countdown(String apNode, Set<String> remainingTables) {

    }

    @Override
    void tryComplete(String apNode, boolean isLastApNode) {
    }

    static class ShowTableByNodeUnitHandler extends GetNodeTablesHandler {
        private final Set<String> expectedTables;
        private final Set<String> ckTables = new HashSet<>();
        private String apNode;

        ShowTableByNodeUnitHandler(Set<String> expectedTables, String apNode) {
            super(apNode);
            this.sql = CLICKHOUSE_SQL;
            this.apNode = apNode;
            this.expectedTables = expectedTables;
        }

        public void execute() {
            ApNode dn = DbleServer.getInstance().getConfig().getApNodes().get(apNode);
            String showTableCol = "name";
            String[] showTableCols = new String[]{showTableCol};
            PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
            if (ds.isAlive()) {
                MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(showTableCols, new ClickHouseShowTablesListener(showTableCol, dn.getDatabase(), ds));
                SQLJob sqlJob = new SQLJob(sql, dn.getDatabase(), resultHandler, ds);
                sqlJob.run();
            } else {
                MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(showTableCols, new ClickHouseShowTablesListener(showTableCol, dn.getDatabase(), null));
                SQLJob sqlJob = new SQLJob(sql, apNode, resultHandler, false);
                sqlJob.run();
            }
        }

        @Override
        protected void handleTable(String table, String tableType) {
            ckTables.add(table);
        }

        private Set<String> getNotExistTable() {
            lock.lock();
            try {
                while (!isFinished) {
                    notify.await();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("getNotExistTable() is interrupted.");
                return Collections.emptySet();
            } finally {
                lock.unlock();
            }
            expectedTables.removeAll(ckTables); // get not exist table
            return expectedTables;
        }

        protected class ClickHouseShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
            private String showTableCol;
            private PhysicalDbInstance ds;
            private String schema;

            ClickHouseShowTablesListener(String showTableCol, String schema, PhysicalDbInstance ds) {
                this.showTableCol = showTableCol;
                this.ds = ds;
                this.schema = schema;
            }

            @Override
            public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
                String key = null;
                if (ds != null) {
                    key = "dbInstance[" + ds.getDbGroupConfig().getName() + "." + ds.getConfig().getInstanceName() + "],ap_node[" + apNode + "],schema[" + schema + "]";
                }
                if (!result.isSuccess()) {
                    //not thread safe
                    String warnMsg = "Can't show tables from apNode:" + apNode + "! Maybe the apNode is not initialized!";
                    LOGGER.warn(warnMsg);
                    if (ds != null) {
                        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", ds.getDbGroupConfig().getName() + "-" + ds.getConfig().getInstanceName());
                        labels.put("ap_node", apNode);
                        AlertUtil.alert(AlarmCode.AP_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", ds.getConfig().getId(), labels);
                        ToResolveContainer.AP_NODE_LACK.add(key);
                    }
                    handleFinished();
                    return;
                }
                if (ds != null && ToResolveContainer.AP_NODE_LACK.contains(key)) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", ds.getDbGroupConfig().getName() + "-" + ds.getConfig().getInstanceName());
                    labels.put("ap_node", apNode);
                    AlertUtil.alertResolve(AlarmCode.AP_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                            ToResolveContainer.AP_NODE_LACK, key);
                }
                List<Map<String, String>> rows = result.getResult();
                for (Map<String, String> row : rows) {
                    String table = row.get(showTableCol);
                    if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                        table = table.toLowerCase();
                    }
                    String tableLackKey = AlertUtil.getTableLackKey2(apNode, table);
                    if (ToResolveContainer.TABLE_LACK.contains(tableLackKey)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableLackKey),
                                ToResolveContainer.TABLE_LACK, tableLackKey);
                    }
                    handleTable(table, null);
                }
                handleFinished();
            }
        }
    }

}
