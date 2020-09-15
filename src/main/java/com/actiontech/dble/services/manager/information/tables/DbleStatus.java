package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.TimeUtil;

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
        Runtime rt = Runtime.getRuntime();
        long memoryMax = rt.maxMemory();
        long memoryTotal = rt.totalMemory();
        long memoryUsed = (memoryTotal - rt.freeMemory());
        BufferPool pool = BufferPoolManager.getBufferPool();
        long poolSize = pool.capacity();
        long poolUsed = poolSize - pool.size();
        TsQueriesCounter.CalculateResult result = TsQueriesCounter.getInstance().calculate();

        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        list.add(genRow("uptime", ((double) upTime / 1000) + "s", "length of time to start dble"));
        list.add(genRow("current_timestamp", FormatUtil.formatDate(now), "the current time of the dble system"));
        list.add(genRow("startup_timestamp", FormatUtil.formatDate(startupTime), "dble system startup time"));
        list.add(genRow("config_reload_timestamp", FormatUtil.formatDate(server.getConfig().getReloadTime()), "last config load time"));
        list.add(genRow("heap_memory_max", memoryMax + "", "the maximum amount of memory that the virtual machine will attempt to use, measured in bytes"));
        list.add(genRow("heap_memory_used", memoryUsed + "", "heap memory usage, measured in bytes"));
        list.add(genRow("heap_memory_total", memoryTotal + "", "the total of heap memory, measured in bytes"));
        list.add(genRow("direct_memory_max", Platform.getMaxDirectMemory() + "", "max direct memory, measured in bytes"));
        list.add(genRow("direct_memory_pool_size", poolSize + "", "size of the memory pool, is equal to the product of BufferPoolPagesize and BufferPoolPagenumber, measured in bytes"));
        list.add(genRow("direct_memory_pool_used", poolUsed + "", "directmemory memory in the memory pool that has been used, measured in bytes"));
        list.add(genRow("questions", result.queries + "", "number of requests"));
        list.add(genRow("transactions", result.transactions + "", "number of transactions"));
        return list;
    }

    private LinkedHashMap<String, String> genRow(String name, String value, String comment) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put(COLUMN_VARIABLE_NAME, name);
        row.put(COLUMN_VARIABLE_VALUE, value);
        row.put(COLUMN_COMMENT, comment);
        return row;
    }
}
