package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DbleStatus extends ManagerBaseTable {
    private static final String TABLE_NAME = "dble_status";

    private static final String COLUMN_VARIABLE_NAME = "variable_name";
    private static final String COLUMN_VARIABLE_VALUE = "variable_value";
    private static final String COLUMN_COMMENT = "comment";

    public DbleStatus() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_VARIABLE_NAME, new ColumnMeta(COLUMN_VARIABLE_NAME, "varchar(24)", false, true));
        columnsType.put(COLUMN_VARIABLE_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VARIABLE_VALUE, new ColumnMeta(COLUMN_VARIABLE_VALUE, "varchar(20)", false));
        columnsType.put(COLUMN_VARIABLE_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_COMMENT, new ColumnMeta(COLUMN_COMMENT, "varchar(200)", true));
        columnsType.put(COLUMN_COMMENT, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        DbleServer server = DbleServer.getInstance();
        long startupTime = server.getStartupTime();
        long now = TimeUtil.currentTimeMillis();
        long upTime = now - startupTime;

        LinkedHashMap<String, String> uptime = Maps.newLinkedHashMap();
        uptime.put(COLUMN_VARIABLE_NAME, "uptime");
        uptime.put(COLUMN_VARIABLE_VALUE, (double) upTime / 1000 + "s");
        uptime.put(COLUMN_COMMENT, "length of time to start dble(s).");

        LinkedHashMap<String, String> currentTimestamp = Maps.newLinkedHashMap();
        currentTimestamp.put(COLUMN_VARIABLE_NAME, "current_timestamp");
        currentTimestamp.put(COLUMN_VARIABLE_VALUE, FormatUtil.formatDate(now));
        currentTimestamp.put(COLUMN_COMMENT, "the current time of the dble system.");

        LinkedHashMap<String, String> startupTimestamp = Maps.newLinkedHashMap();
        startupTimestamp.put(COLUMN_VARIABLE_NAME, "startup_timestamp");
        startupTimestamp.put(COLUMN_VARIABLE_VALUE, FormatUtil.formatDate(server.getStartupTime()));
        startupTimestamp.put(COLUMN_COMMENT, "dble system startup time.");

        Runtime rt = Runtime.getRuntime();
        long memoryTotal = rt.totalMemory();
        long memoryUsed = (memoryTotal - rt.freeMemory());

        LinkedHashMap<String, String> heapMemoryUsed = Maps.newLinkedHashMap();
        heapMemoryUsed.put(COLUMN_VARIABLE_NAME, "heap_memory_used");
        heapMemoryUsed.put(COLUMN_VARIABLE_VALUE, memoryUsed + "mb");
        heapMemoryUsed.put(COLUMN_COMMENT, "heap memory usage(mb)");

        LinkedHashMap<String, String> heapMemoryTotal = Maps.newLinkedHashMap();
        heapMemoryTotal.put(COLUMN_VARIABLE_NAME, "heap_memory_total");
        heapMemoryTotal.put(COLUMN_VARIABLE_VALUE, memoryTotal + "mb");
        heapMemoryTotal.put(COLUMN_COMMENT, "the amount of heap memory(mb)");

        LinkedHashMap<String, String> configReloadTimestamp = Maps.newLinkedHashMap();
        configReloadTimestamp.put(COLUMN_VARIABLE_NAME, "config_reload_timestamp");
        configReloadTimestamp.put(COLUMN_VARIABLE_VALUE, FormatUtil.formatDate(server.getConfig().getReloadTime()));
        configReloadTimestamp.put(COLUMN_COMMENT, "last config load time.");

        BufferPool pool = BufferPoolManager.getBufferPool();
        long poolSize = pool.capacity();

        LinkedHashMap<String, String> directMemoryPoolSize = Maps.newLinkedHashMap();
        directMemoryPoolSize.put(COLUMN_VARIABLE_NAME, "direct_memory_pool_size");
        directMemoryPoolSize.put(COLUMN_VARIABLE_VALUE, poolSize + "");
        directMemoryPoolSize.put(COLUMN_COMMENT, "size of the memory pool, is equal to the product of BufferPoolPagesize and BufferPoolPagenumber.");

        long poolUsed = poolSize - pool.size();

        LinkedHashMap<String, String> directMemoryPoolUsed = Maps.newLinkedHashMap();
        directMemoryPoolUsed.put(COLUMN_VARIABLE_NAME, "direct_memory_pool_used");
        directMemoryPoolUsed.put(COLUMN_VARIABLE_VALUE, poolUsed + "");
        directMemoryPoolUsed.put(COLUMN_COMMENT, "directmemory memory in the memory pool that has been used.");

        TsQueriesCounter.CalculateResult result = TsQueriesCounter.getInstance().calculate();

        LinkedHashMap<String, String> questions = Maps.newLinkedHashMap();
        questions.put(COLUMN_VARIABLE_NAME, "questions");
        questions.put(COLUMN_VARIABLE_VALUE, result.queries + "");
        questions.put(COLUMN_COMMENT, "number of requests.");

        LinkedHashMap<String, String> transactions = Maps.newLinkedHashMap();
        transactions.put(COLUMN_VARIABLE_NAME, "transactions");
        transactions.put(COLUMN_VARIABLE_VALUE, result.transactions + "");
        transactions.put(COLUMN_COMMENT, "the transaction number.");

        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        list.add(uptime);
        list.add(currentTimestamp);
        list.add(startupTimestamp);
        list.add(heapMemoryUsed);
        list.add(heapMemoryTotal);
        list.add(configReloadTimestamp);
        list.add(directMemoryPoolSize);
        list.add(directMemoryPoolUsed);
        list.add(questions);
        list.add(transactions);
        return list;
    }
}
