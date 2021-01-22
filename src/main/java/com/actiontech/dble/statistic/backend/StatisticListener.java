package com.actiontech.dble.statistic.backend;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class StatisticListener {
    private Pattern pattern1 = Pattern.compile("start\\s+transaction", Pattern.CASE_INSENSITIVE);
    private Pattern pattern2 = Pattern.compile("set\\s+autocommit\\s+=\\s+[0-1]", Pattern.CASE_INSENSITIVE);

    private static final StatisticListener INSTANCE = new StatisticListener();
    private volatile boolean enable = false;

    private volatile Map<Session, StatisticRecord> recorders = new HashMap<>(16);

    public void start() {
        if (enable) return;
        enable = true;
        for (IOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (!front.isManager()) {
                    NonBlockingSession session = ((ShardingService) front.getService()).getSession2();
                    if (!session.closed()) {
                        register(session);
                    }
                }
            }
        }
    }

    public void stop() {
        enable = false;
        recorders.clear();
    }

    public void register(NonBlockingSession session) {
        if (enable) {
            if (!recorders.keySet().contains(session)) {
                recorders.put(session, new StatisticRecord(session.getShardingService()));
            }
        }
    }

    public StatisticRecord getRecorder(NonBlockingSession session) {
        if (enable) {
            return recorders.get(session);
        }
        return null;
    }

    public StatisticRecord getRecorder(AbstractService service) {
        if (enable && service instanceof ShardingService) {
            return recorders.get(((ShardingService) service).getSession2());
        }
        return null;
    }

    public void remove(NonBlockingSession session) {
        if (enable) {
            recorders.remove(session);
        }
    }

    public void remove(AbstractService service) {
        if (enable && service instanceof ShardingService) {
            recorders.remove(((ShardingService) service).getSession2());
        }
    }

    private boolean txFilter(String sql) {
        // move: commit、begin、start transaction、set autocommit = 1/0
        if (sql == null) {
            return true;
        } else {
            sql = sql.trim();
            if (StringUtils.containsIgnoreCase(sql, "commit") ||
                    StringUtils.containsIgnoreCase(sql, "begin") ||
                    (sql.toLowerCase().startsWith("start") && pattern1.matcher(sql).find()) ||
                    (sql.toLowerCase().startsWith("set") && pattern2.matcher(sql).find())) {
                return true;
            }
        }
        return false;
    }

    public static StatisticListener getInstance() {
        return INSTANCE;
    }

    public class StatisticRecord {

        private FrontendInfo frontendInfo;
        // tx
        private StatisticTxEntry txEntry;

        public void onTxStartBySet(ShardingService shardingService) {
            onTxStart(shardingService, 0);
        }

        public void onTxStartByBegin(ShardingService shardingService) {
            onTxStart(shardingService, 1);
        }

        // set：0
        // commit：1
        private void onTxStart(ShardingService shardingService, int startType) {
            txEntry = new StatisticTxEntry(frontendInfo, shardingService.getXid(), startType, System.nanoTime());
        }

        public void onTxEndBySet() {
            onTxEnd(0);
        }

        public void onTxEndByCommit() {
            onTxEnd(1);
        }

        // set：0
        // commit：1
        private void onTxEnd(int endType) {
            if (txEntry != null) {
                txEntry.setAllEndTime(System.nanoTime());
                txEntry.setEndType(endType);
                pushTx();
            }
        }

        private void onTxData(StatisticFrontendSqlEntry frontendSqlentry) {
            if (txEntry != null && txEntry.getTxId() > 0 && !txFilter(frontendSqlentry.getSql())) {
                txEntry.add(frontendSqlentry);
            }
        }

        // frontend sql
        private StatisticFrontendSqlEntry frontendSqlEntry;
        private volatile boolean init = false;

        // Frontend
        public void onFrontendSqlStart() {
            frontendSqlEntry = new StatisticFrontendSqlEntry(frontendInfo);
            frontendSqlEntry.setStartTime(System.nanoTime());
            if (txEntry != null) {
                frontendSqlEntry.setTxId(txEntry.getTxId());
            }
            init = true;
        }

        public void onFrontendSetSql(String schema, String sql) {
            if (init && frontendSqlEntry != null) {
                if (sql != null) {
                    frontendSqlEntry.setSql(sql);
                    frontendSqlEntry.setSchema(schema);
                } else {
                    onFrontendSqlClose();
                }
            }
        }

        public void onFrontendSqlClose() {
            if (init && frontendSqlEntry != null) {
                init = false;
                frontendSqlEntry = null;
            }
        }

        public void onFrontendSetRows(long rows) {
            if (init && frontendSqlEntry != null) {
                frontendSqlEntry.setRows(rows);
            }
        }

        public void onFrontendAddRows() {
            if (init && frontendSqlEntry != null) {
                frontendSqlEntry.addRows();
            }
        }

        public void onFrontendSqlEnd() {
            if (init && frontendSqlEntry != null) {
                if (frontendSqlEntry.getSql() == null) {
                    onFrontendSqlClose();
                }
                frontendSqlEntry.setAllEndTime(System.nanoTime());
                init = false;
                pushFrontendSql();
            }
        }

        // Backend
        public void onBackendSqlStart(MySQLResponseService service) {
            if (init && frontendSqlEntry != null) {
                RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                BackendConnection connection = service.getConnection();
                ShardingService shardingService = service.getSession().getShardingService();

                StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                        frontendInfo,
                        ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), node.getName(),
                        node.getSqlType(), node.getStatement(), System.nanoTime());

                //if (shardingService.isTxStart() || !shardingService.isAutocommit()) {
                if (txEntry != null) {
                    entry.setTxId(shardingService.getXid());
                }
                String key = connection.getId() + ":" + node.getName() + ":" + +node.getStatementHash();
                frontendSqlEntry.put(key, entry);
            }
        }

        /*public void onBackendSqlFirstEnd(MySQLResponseService service) {
            if (init && frontendSqlEntry != null) {
                RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
                if (frontendSqlEntry != null && frontendSqlEntry.getBackendSqlEntry(key) != null && frontendSqlEntry.getBackendSqlEntry(key).getFirstEndTime() == 0L) {
                    frontendSqlEntry.getBackendSqlEntry(key).setFirstEndTime(System.nanoTime());
                }
            }
        }*/

        public void onBackendSqlSetRows(MySQLResponseService service, long rows) {
            if (init && frontendSqlEntry != null) {
                RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                String key = service.getConnection().getId() + ":" + node.getName() + ":" + +node.getStatementHash();
                if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                    frontendSqlEntry.getBackendSqlEntry(key).setRows(rows);
                    frontendSqlEntry.addExaminedRows(rows);
                }
            }
        }

        public void onBackendSqlAddRows(MySQLResponseService service) {
            if (init && frontendSqlEntry != null) {
                RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                String key = service.getConnection().getId() + ":" + node.getName() + ":" + +node.getStatementHash();
                if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                    frontendSqlEntry.getBackendSqlEntry(key).addRows();
                    frontendSqlEntry.addExaminedRows();
                }
            }
        }

        public void onBackendSqlEnd(MySQLResponseService service) {
            if (init && frontendSqlEntry != null) {
                RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                String key = service.getConnection().getId() + ":" + node.getName() + ":" + +node.getStatementHash();
                if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                    frontendSqlEntry.getBackendSqlEntry(key).setAllEndTime(System.nanoTime());
                    pushBackendSql(frontendSqlEntry.getBackendSqlEntry(key));
                }
            }
        }

        // push
        private void pushBackendSql(final StatisticBackendSqlEntry backendSqlEntry) {
            if (backendSqlEntry != null) {
                StatisticManager.getInstance().push(backendSqlEntry);
            }
        }

        private void pushFrontendSql() { // add to tx and push
            if (frontendSqlEntry != null) {
                onTxData(frontendSqlEntry);
                StatisticManager.getInstance().push(frontendSqlEntry);

            }
        }

        private void pushTx() {
            if (txEntry != null) {
                StatisticManager.getInstance().push(txEntry);
            }
        }

        public StatisticRecord(ShardingService shardingService) {
            this.frontendInfo = new FrontendInfo(shardingService.getUser().getName(),
                    shardingService.getConnection().getHost(),
                    shardingService.getConnection().getPort());
        }
    }
}
