package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.check.GlobalCheckJob;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2019/12/25.
 */
public final class CheckGlobalConsistency {

    private Map<String, List<ConsistencyResult>> resultMap = new ConcurrentHashMap<>();
    private AtomicInteger counter;
    private final ManagerService service;
    private final List<GlobalCheckJob> globalCheckJobs;
    private final ReentrantLock lock = new ReentrantLock();


    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final Pattern PATTERN = Pattern.compile("check\\s+@@global(\\s+schema\\s*=\\s*\\'(([^'])+)\\'(\\s+and\\s+table\\s*=\\s*\\'([^']+)\\')?)?", Pattern.CASE_INSENSITIVE);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TABLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DISTINCT_CONSISTENCY_NUMBER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ERROR_NODE_NUMBER", Fields.FIELD_TYPE_LONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private CheckGlobalConsistency(List<GlobalCheckJob> jobs, ManagerService service) {

        this.service = service;
        this.globalCheckJobs = jobs;
    }

    public static void execute(ManagerService con, String stmt) {
        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches()) {
            con.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql does not match: check @@global schema = ? and table = ?");
            return;
        }

        String schema = ma.group(2);
        String table = ma.group(5);

        Map<String, SchemaConfig> schemaConfigs = DbleServer.getInstance().getConfig().getSchemas();

        List<GlobalCheckJob> jobs = new ArrayList<>();
        CheckGlobalConsistency consistencyCheck = new CheckGlobalConsistency(jobs, con);
        if (schema != null) {
            SchemaConfig sc = schemaConfigs.get(schema);
            if (sc == null) {
                con.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "schema must exists");
                return;
            } else {
                if (table != null) {
                    String[] tables = table.split(",");
                    for (String singleTable : tables) {
                        BaseTableConfig config = sc.getTables().get(singleTable);
                        if (config == null || !(config instanceof GlobalTableConfig) || !((GlobalTableConfig) config).isGlobalCheck()) {
                            con.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "tables must exist and must be global table with global check");
                            return;
                        } else {
                            jobs.add(new GlobalCheckJob((GlobalTableConfig) config, schema, consistencyCheck));
                        }
                    }
                } else {
                    for (Map.Entry<String, BaseTableConfig> te : sc.getTables().entrySet()) {
                        BaseTableConfig config = te.getValue();
                        if ((config instanceof GlobalTableConfig) && ((GlobalTableConfig) config).isGlobalCheck()) {
                            jobs.add(new GlobalCheckJob((GlobalTableConfig) config, schema, consistencyCheck));
                        }
                    }
                }
            }
        } else {
            for (Map.Entry<String, SchemaConfig> se : schemaConfigs.entrySet()) {
                for (Map.Entry<String, BaseTableConfig> te : se.getValue().getTables().entrySet()) {
                    BaseTableConfig config = te.getValue();
                    if ((config instanceof GlobalTableConfig) && ((GlobalTableConfig) config).isGlobalCheck()) {
                        jobs.add(new GlobalCheckJob((GlobalTableConfig) config, se.getKey(), consistencyCheck));
                    }
                }
            }
        }
        if (jobs.size() == 0) {
            consistencyCheck.response();
        } else {
            consistencyCheck.start();
        }
    }

    private void response() {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);
        // write rows
        byte packetId = EOF.getPacketId();
        for (Map.Entry<String, List<ConsistencyResult>> entry : resultMap.entrySet()) {
            for (ConsistencyResult cr : entry.getValue()) {
                RowDataPacket row = getRow(entry.getKey(), cr);
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }


    private void start() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("global-check-start");
        try {
            if (globalCheckJobs.size() == 0) {
                response();
                return;
            }
            counter = new AtomicInteger(globalCheckJobs.size());
            for (GlobalCheckJob job : globalCheckJobs) {
                job.checkGlobalTable();
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private RowDataPacket getRow(String schema, ConsistencyResult cr) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        String charset = service.getCharset().getResults();
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(cr.table, charset));
        row.add(LongUtil.toBytes(cr.distinctNo));
        row.add(LongUtil.toBytes(cr.errorNo));
        return row;
    }


    public void collectResult(String schema, String table, int distinctNo, int errorNo) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("single-table-global-finish");
        lock.lock();
        try {
            List<ConsistencyResult> list = resultMap.computeIfAbsent(schema, k -> Collections.synchronizedList(new ArrayList<>()));
            list.add(new ConsistencyResult(table, distinctNo, errorNo));
        } finally {
            lock.unlock();
            TraceManager.finishSpan(traceObject);
        }
        if (counter.decrementAndGet() <= 0) {
            response();
        }
    }

    static class ConsistencyResult {
        final String table;
        final int distinctNo;
        final int errorNo;

        ConsistencyResult(String table, int distinctNo, int errorNo) {
            this.table = table;
            this.distinctNo = distinctNo;
            this.errorNo = errorNo;
        }

    }
}
