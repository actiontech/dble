/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SqlStatisticHandler implements StatisticDataHandler {

    private final ConcurrentSkipListMap<Long, TxRecord> txRecords = new ConcurrentSkipListMap<>();
    private AtomicLong txRecordSize = new AtomicLong(0);
    private volatile BitSet sampleDecisions;

    public SqlStatisticHandler() {
        this.sampleDecisions = randomBitSet(SystemConfig.getInstance().getSamplingRate(), new Random());
    }

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) {
        if (StatisticManager.getInstance().getSamplingRate() == 0) {
            return;
        }
        handle(statisticEvent.getEntry());
    }

    public void handle(StatisticEntry entry) {
        if (entry instanceof StatisticFrontendSqlEntry) {
            StatisticFrontendSqlEntry frontendSqlEntry = (StatisticFrontendSqlEntry) entry;
            if (sampleDecisions.get((int) (frontendSqlEntry.getTxId() % 100))) {
                if (null == txRecords.get(frontendSqlEntry.getTxId())) {
                    if (txRecordSize.intValue() >= StatisticManager.getInstance().getSqlLogSize()) {
                        txRecords.pollFirstEntry();
                        txRecordSize.decrementAndGet();
                    }
                    txRecords.put(frontendSqlEntry.getTxId(), new TxRecord(frontendSqlEntry));
                    txRecordSize.incrementAndGet();
                    checkEliminate();
                } else {
                    txRecords.get(frontendSqlEntry.getTxId()).getSqls().add(new SQLRecord(frontendSqlEntry));
                }
            }
        }
    }

    private void checkEliminate() {
        int removeIndex;
        if ((removeIndex = txRecordSize.intValue() - StatisticManager.getInstance().getSqlLogSize()) > 0) {
            while (removeIndex-- > 0) {
                txRecords.pollFirstEntry();
                txRecordSize.decrementAndGet();
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
        txRecordSize.set(0);
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

        private long resultSize;
        // ns
        private long duration;
        private long startTime;

        private AtomicBoolean init = new AtomicBoolean(false);

        public SQLRecord(StatisticFrontendSqlEntry entry) {
            this.sqlId = SQL_ID_GENERATOR.incrementAndGet();
            this.stmt = entry.getSql();
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
            this.resultSize = entry.getResultSize();
            // rows
            this.rows = entry.getRows();
            this.examinedRows = entry.getExaminedRows();
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
            if (init.compareAndSet(false, true)) {
                try {
                    if (stmt.equalsIgnoreCase("begin")) {
                        this.sqlDigest = "begin";
                    } else {
                        String tmpStmt = ParameterizedOutputVisitorUtils.parameterize(this.stmt, DbType.mysql);
                        this.sqlDigest = tmpStmt.replaceAll("[\\t\\n\\r]", " ");
                    }
                } catch (RuntimeException e) {
                    this.sqlDigest = "Other";
                }
            }
            return sqlDigest;
        }

        public long getResultSize() {
            return resultSize;
        }
    }

}
