/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.handler;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.statistic.sql.StatisticEvent;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.sql.entry.FrontendInfo;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticTxEntry;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SqlStatisticHandler implements StatisticDataHandler {

    private final ConcurrentSkipListMap<Long, TxRecord> txRecords = new ConcurrentSkipListMap<>();
    private volatile BitSet sampleDecisions;

    public SqlStatisticHandler() {
        this.sampleDecisions = randomBitSet(SystemConfig.getInstance().getSamplingRate(), new Random());
    }

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) {
        if (StatisticManager.getInstance().getSamplingRate() == 0) {
            return;
        }

        StatisticEntry entry = statisticEvent.getEntry();
        if (entry instanceof StatisticTxEntry) {
            StatisticTxEntry txEntry = (StatisticTxEntry) entry;
            if (sampleDecisions.get((int) (txEntry.getTxId() % 100))) {
                if (null == txRecords.get(txEntry.getTxId())) {
                    if (txRecords.size() >= StatisticManager.getInstance().getSqlLogSize()) {
                        txRecords.pollFirstEntry();
                    }
                    txRecords.put(txEntry.getTxId(), new TxRecord(txEntry));
                } else {
                    txRecords.get(txEntry.getTxId()).addSqls(txEntry.getEntryList());
                }
            }
        } else if (entry instanceof StatisticFrontendSqlEntry) {
            StatisticFrontendSqlEntry frontendSqlEntry = (StatisticFrontendSqlEntry) entry;
            if (!frontendSqlEntry.isNeedToTx()) {
                return;
            }
            if (sampleDecisions.get((int) (frontendSqlEntry.getTxId() % 100))) {
                if (null == txRecords.get(frontendSqlEntry.getTxId())) {
                    if (txRecords.size() >= StatisticManager.getInstance().getSqlLogSize()) {
                        txRecords.pollFirstEntry();
                    }
                    txRecords.put(frontendSqlEntry.getTxId(), new TxRecord(frontendSqlEntry));
                } else {
                    txRecords.get(frontendSqlEntry.getTxId()).getSqls().add(new SQLRecord(frontendSqlEntry));
                }
            }
        }
    }

    private void checkEliminate() {
        int removeIndex;
        if ((removeIndex = txRecords.size() - StatisticManager.getInstance().getSqlLogSize()) > 0) {
            while (removeIndex-- > 0) {
                txRecords.pollFirstEntry();
            }
        }
    }

    @Override
    public LinkedList<TxRecord> getList() {
        checkEliminate();
        return new LinkedList<>(txRecords.values());
    }

    @Override
    public void clear() {
        txRecords.clear();
    }

    private BitSet randomBitSet(int cardinality, Random rnd) {
        if (cardinality < 0) throw new IllegalArgumentException();
        BitSet result = new BitSet(100);
        int[] chosen = new int[cardinality];
        int i;
        for (i = 0; i < cardinality; ++i) {
            chosen[i] = i;
            result.set(i);
        }
        for (; i < 100; ++i) {
            int j = rnd.nextInt(i + 1);
            if (j < cardinality) {
                result.clear(chosen[j]);
                result.set(i);
                chosen[j] = i;
            }
        }
        return result;
    }

    public void setSampleDecisions(int samplingRate) {
        sampleDecisions = randomBitSet(samplingRate, new Random());
    }

    public static class TxRecord {
        private final long startTime;
        private final long duration;
        private final FrontendInfo info;
        private final List<SQLRecord> sqls;

        TxRecord(StatisticFrontendSqlEntry frontendSqlEntry) {
            this.startTime = frontendSqlEntry.getStartTimeMs();
            this.info = frontendSqlEntry.getFrontend();
            this.duration = frontendSqlEntry.getDuration();
            this.sqls = new ArrayList<>(2);
            this.sqls.add(new SQLRecord(frontendSqlEntry));
        }

        TxRecord(StatisticTxEntry txEntry) {
            this.startTime = txEntry.getStartTimeMs();
            this.info = txEntry.getFrontend();
            this.duration = txEntry.getDuration();
            final List<StatisticFrontendSqlEntry> entryList = txEntry.getEntryList();
            this.sqls = new ArrayList<>();
            for (StatisticFrontendSqlEntry sql : entryList) {
                this.sqls.add(new SQLRecord(sql));
            }
        }

        public void addSqls(final List<StatisticFrontendSqlEntry> entryList) {
            if (this.sqls == null) return;
            for (StatisticFrontendSqlEntry sql : entryList) {
                this.sqls.add(new SQLRecord(sql));
            }
        }

        public long getStartTime() {
            return startTime;
        }

        public FrontendInfo getInfo() {
            return info;
        }

        public long getDuration() {
            return duration;
        }

        public List<SQLRecord> getSqls() {
            return sqls;
        }

    }

    public static class SQLRecord {

        private static final AtomicLong SQL_ID_GENERATOR = new AtomicLong(0);

        private long sqlId;
        private String stmt;
        private String sqlDigest;
        private int sqlType;
        private long txId;
        private String sourceHost;
        private int sourcePort;

        private String user;
        private int entry;

        private long rows;
        private long examinedRows;
        // ns
        private long duration;
        private long startTime;

        public SQLRecord(StatisticFrontendSqlEntry entry) {
            this.sqlId = SQL_ID_GENERATOR.incrementAndGet();
            this.stmt = entry.getSql();
            if (stmt.equalsIgnoreCase("begin")) {
                this.sqlDigest = "begin";
            } else {
                try {
                    this.sqlDigest = ParameterizedOutputVisitorUtils.parameterize(this.stmt, DbType.mysql).replaceAll("[\\t\\n\\r]", " ");
                } catch (RuntimeException e) {
                    this.sqlDigest = "Other";
                }
            }
            this.sqlType = entry.getSqlType();
            this.txId = entry.getTxId();

            // Front connection info
            final FrontendInfo frontendInfo = entry.getFrontend();
            this.sourceHost = frontendInfo.getHost();
            this.sourcePort = frontendInfo.getPort();
            this.user = frontendInfo.getUser();
            this.entry = frontendInfo.getUserId();
            // time
            this.startTime = entry.getStartTimeMs();
            this.duration = entry.getDuration();
            // rows
            this.rows = entry.getRows();
            this.examinedRows = entry.getExaminedRows().longValue();
        }

        public long getSqlId() {
            return sqlId;
        }

        public String getStmt() {
            return stmt;
        }

        public long getTxId() {
            return txId;
        }

        public int getSqlType() {
            return sqlType;
        }

        public String getSourceHost() {
            return sourceHost;
        }

        public int getSourcePort() {
            return sourcePort;
        }

        public long getRows() {
            return rows;
        }

        public long getDuration() {
            return duration;
        }

        public long getStartTime() {
            return startTime;
        }

        public String getUser() {
            return user;
        }

        public int getEntry() {
            return entry;
        }

        public long getExaminedRows() {
            return examinedRows;
        }

        public String getSqlDigest() {
            return sqlDigest;
        }
    }

}
