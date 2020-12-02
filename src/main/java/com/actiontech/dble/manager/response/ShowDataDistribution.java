/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.SingleTableConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class ShowDataDistribution {
    private ShowDataDistribution() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SHARDING_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("COUNT", Fields.FIELD_TYPE_LONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, String name) {
        if (!name.startsWith("'") || !name.endsWith("'")) {
            c.writeErrMessage(ErrorCode.ER_YES, "The query should be show @@data_distribution where table ='schema.table'");
            return;
        }
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            name = name.toLowerCase();
        }
        String[] schemaInfo = name.substring(1, name.length() - 1).split("\\.");
        if (schemaInfo.length != 2) {
            c.writeErrMessage(ErrorCode.ER_YES, "The query should be show @@data_distribution where table ='schema.table'");
            return;
        }
        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaInfo[0]);
        if (schemaConfig == null) {
            c.writeErrMessage(ErrorCode.ER_YES, "The schema " + schemaInfo[0] + " doesn't exist");
            return;
        } else if (schemaConfig.isNoSharding()) {
            c.writeErrMessage(ErrorCode.ER_YES, "The schema " + schemaInfo[0] + " is no sharding schema");
            return;
        }
        BaseTableConfig tableConfig = schemaConfig.getTables().get(schemaInfo[1]);
        if (tableConfig == null) {
            c.writeErrMessage(ErrorCode.ER_YES, "The table " + name + " doesn't exist");
            return;
        } else if (tableConfig instanceof SingleTableConfig) {
            c.writeErrMessage(ErrorCode.ER_YES, "The table " + name + " is Single table");
            return;
        }
        ReentrantLock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        Map<String, Integer> results = new ConcurrentHashMap<>();
        AtomicBoolean succeed = new AtomicBoolean(true);
        for (String shardingNode : tableConfig.getShardingNodes()) {
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[]{"COUNT"}, new ShowDataDistributionListener(shardingNode, lock, cond, results, succeed));
            SQLJob sqlJob = new SQLJob("SELECT COUNT(*) AS COUNT FROM " + schemaInfo[1], shardingNode, resultHandler, true);
            sqlJob.run();
        }
        lock.lock();
        try {
            while (results.size() != tableConfig.getShardingNodes().size()) {
                cond.await();
            }
        } catch (InterruptedException e) {
            c.writeErrMessage(ErrorCode.ER_YES, "occur InterruptedException, so try again later ");
            return;
        } finally {
            lock.unlock();
        }

        if (!succeed.get()) {
            c.writeErrMessage(ErrorCode.ER_YES, "occur Exception, so see dble.log to check reason");
            return;
        }
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.getPacketId();
        Map<String, Integer> orderResults = new TreeMap<>(results);

        for (Map.Entry<String, Integer> entry : orderResults.entrySet()) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(entry.getKey(), c.getCharset().getResults()));
            row.add(IntegerUtil.toBytes(entry.getValue()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static class ShowDataDistributionListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private ReentrantLock lock;
        private Condition cond;
        private Map<String, Integer> results;
        private AtomicBoolean succeed;
        private String shardingNode;

        ShowDataDistributionListener(String shardingNode, ReentrantLock lock, Condition cond, Map<String, Integer> results, AtomicBoolean succeed) {
            this.shardingNode = shardingNode;
            this.lock = lock;
            this.cond = cond;
            this.results = results;
            this.succeed = succeed;

        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (!result.isSuccess()) {
                succeed.set(false);
                results.put(shardingNode, 0);
            } else {
                String count = result.getResult().get("COUNT");
                results.put(shardingNode, Integer.valueOf(count));
            }
            lock.lock();
            try {
                cond.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
