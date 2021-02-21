package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.statistic.sql.entry.*;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

public class StatisticRecord {

    private Pattern pattern1 = Pattern.compile("start\\s+transaction", Pattern.CASE_INSENSITIVE);
    private Pattern pattern2 = Pattern.compile("set\\s+(autocommit|xa)\\s*=\\s*[0-1]", Pattern.CASE_INSENSITIVE);

    private FrontendInfo frontendInfo;

    // xa
    private volatile String xaId = null;

    public void onXaStart(String id) {
        xaId = id;
    }

    public void onXaStop() {
        xaId = null;
    }

    private void onXaData(StatisticFrontendSqlEntry entry) {
        if (entry.getXaId() != null) {
            StatisticTxEntry xaEntry = new StatisticTxEntry(frontendInfo, xaId, entry.getTxId(), 0, entry.getStartTime());
            xaEntry.setAllEndTime(entry.getAllEndTime());
            xaEntry.add(entry);
            pushXa(xaEntry);
        }
    }

    // tx
    private volatile StatisticTxEntry txEntry;
    private volatile boolean isStartTx;

    public void onTxStartBySet(BusinessService businessService) {
        onTxStart(businessService, 0);
    }

    public void onTxStartByBegin(BusinessService businessService) {
        onTxStart(businessService, 1);
    }

    // startType
    // set：0
    // commit：1
    private void onTxStart(BusinessService businessService, int startType) {
        if (businessService instanceof ShardingService) {
            isStartTx = true;
            txEntry = new StatisticTxEntry(frontendInfo, xaId, ((ShardingService) businessService).getXid(), startType, System.nanoTime());
        } else if (businessService instanceof RWSplitService) {
            isStartTx = true;
            txEntry = new StatisticTxEntry(frontendInfo, xaId, ((RWSplitService) businessService).getTxId(), startType, System.nanoTime());
        }
    }

    public void onTxEndBySet() {
        onTxEnd(0);
    }

    public void onTxEndByCommit() {
        onTxEnd(1);
    }

    public void onTxEndByRollback() {
        onTxEnd(2);
    }

    /*public void onTxEndByInterrupt() {
        onTxEnd(3);
    }*/


    // set：0
    // commit：1
    private void onTxEnd(int endType) {
        if (isStartTx && txEntry != null) {
            isStartTx = false;
            txEntry.setAllEndTime(System.nanoTime());
            txEntry.setEndType(endType);
            pushTx();
        }
    }

    private void onTxData(StatisticFrontendSqlEntry frontendSqlentry) {
        if (isStartTx && txEntry != null && txEntry.getTxId() > 0) {
            if (txFilterDdl(frontendSqlentry.getSqlType())) return;
            txEntry.add(frontendSqlentry);
        } else if (xaId != null) {
            if (txFilterDdl(frontendSqlentry.getSqlType())) return;
            onXaData(frontendSqlentry);
        }
    }


    // frontend sql
    private volatile StatisticFrontendSqlEntry frontendSqlEntry;
    private volatile boolean isStartFsql = false;

    public void onFrontendSqlStart() {
        isStartFsql = true;
        frontendSqlEntry = new StatisticFrontendSqlEntry(frontendInfo, txEntry != null ? txEntry.getTxId() : -1L, System.nanoTime());
        if (xaId != null) {
            frontendSqlEntry.setXaId(xaId);
        }
    }

    public void onFrontendSetSql(String schema, String sql) {
        if (isStartFsql && frontendSqlEntry != null) {
            if (!txFilter(sql)) {
                frontendSqlEntry.setSql(sql);
                frontendSqlEntry.setSchema(schema);
                if (txEntry != null) {
                    frontendSqlEntry.setTxId(txEntry.getTxId());
                }
            } else {
                onFrontendSqlClose();
            }
        }
    }

    public void onFrontendSqlClose() {
        if (isStartFsql && frontendSqlEntry != null) {
            if (frontendSqlEntry.getBackendSqlEntrys().size() <= 0) {
                isStartFsql = false;
                frontendSqlEntry = null;
            }
        }
    }

    public void onFrontendSetRows(long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.setRows(rows);
        }
    }

    public void onFrontendAddRows() {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.addRows();
        }
    }

    public void onFrontendSqlEnd() {
        if (isStartFsql && frontendSqlEntry != null) {
            if (frontendSqlEntry.getSql() == null) {
                onFrontendSqlClose();
            } else {
                frontendSqlEntry.setAllEndTime(System.nanoTime());
                isStartFsql = false;
                onTxData(frontendSqlEntry);
                pushFrontendSql();
            }
        }
    }


    // Backend sql
    public void onBackendSqlStart(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            BackendConnection connection = service.getConnection();
            ShardingService shardingService = service.getSession().getShardingService();

            StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                    frontendInfo,
                    ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), node.getName(),
                    node.getSqlType(), node.getStatement(), System.nanoTime());

            if (txEntry != null) {
                entry.setTxId(shardingService.getXid());
            }
            String key = connection.getId() + ":" + node.getName() + ":" + node.getStatementHash();
            frontendSqlEntry.put(key, entry);
        }
    }

    public void onRWBackendSqlStart(BackendConnection connection) {
        if (isStartFsql && frontendSqlEntry != null) {
            StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                    frontendInfo,
                    ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), "-",
                    frontendSqlEntry.getSql(), System.nanoTime());
            frontendSqlEntry.put("&statistic_rw_key", entry);
        }
    }

    public void onBackendSqlFirstEnd(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && frontendSqlEntry.getBackendSqlEntry(key).getFirstEndTime() == 0L) {
                frontendSqlEntry.getBackendSqlEntry(key).setFirstEndTime(System.nanoTime());
            }
        }
    }

    public void onBackendSqlSetRows(MySQLResponseService service, long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).setRows(rows);
                frontendSqlEntry.addExaminedRows(rows);
            }
        }
    }

    public void onBackendSqlAddRows(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).addRows();
                frontendSqlEntry.addExaminedRows();
            }
        }
    }

    public void onBackendSqlEnd(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).setAllEndTime(System.nanoTime());
                pushBackendSql(frontendSqlEntry.getBackendSqlEntry(key));
            }
        }
    }

    public void onRWBackendSqlSetRowsAndEnd(long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setRows(rows);
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setAllEndTime(System.nanoTime());
            pushBackendSql(frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key"));
            frontendSqlEntry.setRowsAndExaminedRows(rows);
            onFrontendSqlEnd();
        }
    }

    public void onRWBackendSqlError(byte[] data) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(data);
        if (errPg.getErrNo() == ErrorCode.ER_PARSE_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_NO_SUCH_TABLE ||
                errPg.getErrNo() == ErrorCode.ER_NO_DB_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_BAD_DB_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_DERIVED_MUST_HAVE_ALIAS) {
            onFrontendSqlClose();
            return;
        }
        onRWBackendSqlSetRowsAndEnd(0);
    }

    public void onRWBackendSqlClose() {
        onFrontendSqlClose();
    }

    // push data
    private void pushBackendSql(StatisticBackendSqlEntry backendSqlEntry) {
        if (backendSqlEntry != null) {
            StatisticManager.getInstance().push(backendSqlEntry);
        }
    }

    private void pushFrontendSql() {
        if (frontendSqlEntry != null) {
            StatisticManager.getInstance().push(frontendSqlEntry);
            frontendSqlEntry = null;
        }
    }

    private void pushTx() {
        if (txEntry != null) {
            if (txEntry.getEntryList().size() != 0) {
                StatisticManager.getInstance().push(txEntry);
            }
            txEntry = null;
        }
    }

    private void pushXa(StatisticTxEntry xaEntry) {
        StatisticManager.getInstance().push(xaEntry);
    }

    private boolean txFilter(String sql) {
        // move: null、commit、begin、start transaction、set autocommit = 1/0
        if (sql == null) {
            return true;
        } else {
            sql = sql.trim();
            if (StringUtils.containsIgnoreCase(sql, "commit") ||
                    StringUtils.containsIgnoreCase(sql, "begin") ||
                    (sql.toLowerCase().startsWith("start") && pattern1.matcher(sql).find()) ||
                    (sql.toLowerCase().startsWith("set") && pattern2.matcher(sql).find()) ||
                    (sql.toLowerCase().startsWith("explain"))) {
                return true;
            }
        }
        return false;
    }

    private boolean txFilterDdl(int sqlType) {
        if (sqlType == ServerParse.DDL) {
            return true;
        }
        return false;
    }

    public StatisticRecord(BusinessService service) {
        this.frontendInfo = new FrontendInfo(service.getUserConfig().getId(),
                service.getUser().getName(),
                service.getConnection().getHost(),
                service.getConnection().getPort());
    }
}
