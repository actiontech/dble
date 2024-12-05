/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.memory.unsafe.Platform;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;
import com.oceanbase.obsharding_d.singleton.TsQueriesCounter;
import com.oceanbase.obsharding_d.util.FormatUtil;
import com.oceanbase.obsharding_d.util.TimeUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DStatus extends ManagerBaseTable {
    private static final String TABLE_NAME = "obsharding_d_status";

    private static final String COLUMN_VARIABLE_NAME = "variable_name";
    private static final String COLUMN_VARIABLE_VALUE = "variable_value";
    private static final String COLUMN_COMMENT = "comment";

    public OBsharding_DStatus() {
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
        OBsharding_DServer server = OBsharding_DServer.getInstance();
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
        list.add(genRow("uptime", ((double) upTime / 1000) + "s", "Length of time to start OBsharding-D"));
        list.add(genRow("current_timestamp", FormatUtil.formatDate(now), "The current time of the OBsharding-D system"));
        list.add(genRow("startup_timestamp", FormatUtil.formatDate(startupTime), "OBsharding-D system startup time"));
        list.add(genRow("config_reload_timestamp", FormatUtil.formatDate(server.getConfig().getReloadTime()), "Last config load time"));
        list.add(genRow("heap_memory_max", memoryMax + "B", "The maximum amount of memory that the virtual machine will attempt to use, measured in bytes"));
        list.add(genRow("heap_memory_used", memoryUsed + "B", "Heap memory usage, measured in bytes"));
        list.add(genRow("heap_memory_total", memoryTotal + "B", "The total of heap memory, measured in bytes"));
        list.add(genRow("direct_memory_max", Platform.getMaxDirectMemory() + "B", "Max direct memory, measured in bytes"));
        list.add(genRow("direct_memory_pool_size", poolSize + "B", "Size of the memory pool, is equal to the product of BufferPoolPagesize and BufferPoolPagenumber, measured in bytes"));
        list.add(genRow("direct_memory_pool_used", poolUsed + "B", "DirectMemory memory in the memory pool that has been used, measured in bytes"));
        list.add(genRow("questions", result.queries + "", "Number of requests"));
        list.add(genRow("transactions", result.transactions + "", "Number of transactions"));
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
