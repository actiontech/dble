/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.executor.*;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public final class DbleThreadPool extends ManagerWritableTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbleThreadPool.class);


    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_SIZE = "size";
    public static final String COLUMN_ACTIVE_COUNT = "active_count";
    private static final String COLUMN_WAITING_TASK_COUNT = "waiting_task_count";

    public DbleThreadPool() {
        super("dble_thread_pool", 4);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(32)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SIZE, new ColumnMeta(COLUMN_SIZE, "int(11)", false));
        columnsType.put(COLUMN_SIZE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ACTIVE_COUNT, new ColumnMeta(COLUMN_ACTIVE_COUNT, "int(11)", false));
        columnsType.put(COLUMN_ACTIVE_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_WAITING_TASK_COUNT, new ColumnMeta(COLUMN_WAITING_TASK_COUNT, "int(11)", false));
        columnsType.put(COLUMN_WAITING_TASK_COUNT, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        DbleServer server = DbleServer.getInstance();
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);
        lst.add(getRow((NameableExecutor) server.getTimerExecutor()));
        lst.add(getRow((NameableExecutor) server.getBusinessExecutor()));
        lst.add(getRow((NameableExecutor) server.getBackendBusinessExecutor()));
        lst.add(getRow((NameableExecutor) server.getComplexQueryExecutor()));
        lst.add(getRow((NameableExecutor) server.getWriteToBackendExecutor()));
        return lst;
    }

    private LinkedHashMap<String, String> getRow(NameableExecutor exec) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put(COLUMN_NAME, exec.getName());
        row.put(COLUMN_SIZE, exec.getPoolSize() + "");
        row.put(COLUMN_ACTIVE_COUNT, exec.getActiveCount() + "");
        row.put(COLUMN_WAITING_TASK_COUNT, exec.getQueue().size() + "");
        return row;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        throw new SQLException("not support insert", "42S22", ErrorCode.ER_DUP_ENTRY);
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        if (values.containsKey(COLUMN_NAME) || values.containsKey(COLUMN_ACTIVE_COUNT) || values.containsKey(COLUMN_WAITING_TASK_COUNT)) {
            throw new SQLException("Column '" + COLUMN_NAME + "/" + COLUMN_ACTIVE_COUNT + "/" + COLUMN_WAITING_TASK_COUNT + "' is not writable", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
        }
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            int oldSize = Integer.parseInt(affectPk.get(COLUMN_SIZE));
            String newSizeStr = values.get(COLUMN_SIZE);
            int newSize = Integer.parseInt(newSizeStr);
            NameableExecutor nameableExecutor = getExecutor(affectPk.get(COLUMN_NAME));
            if (null == nameableExecutor) {
                throw new SQLException("the current line does not support modification", "42S22", ErrorCode.ER_DUP_ENTRY);
            }
            nameableExecutor.setCorePoolSize(newSize);
            if (!nameableExecutor.getName().equals(DbleServer.COMPLEX_QUERY_EXECUTOR_NAME)) {
                nameableExecutor.setMaximumPoolSize(newSize);
            }
            if (oldSize == newSize) {
                continue;
            } else if (oldSize < newSize) {
                //扩容
                increasePoolSize(nameableExecutor, newSize - oldSize);
            } else {
                //缩容
                decreasePoolSize(nameableExecutor, oldSize - newSize);
            }
        }
        return 0;
    }

    private void decreasePoolSize(NameableExecutor nameableExecutor, int decreaseVal) {
        DbleServer server = DbleServer.getInstance();
        Map<Thread, Runnable> runnableMap = server.getRunnableMap().get(nameableExecutor.getName());
        Iterator<Map.Entry<Thread, Runnable>> iterator = runnableMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Thread, Runnable> threadRunnableEntry = iterator.next();
            Thread thread = threadRunnableEntry.getKey();
            Runnable runnable = threadRunnableEntry.getValue();
            if (runnable instanceof Runnable && !thread.isInterrupted() && !nameableExecutor.getName().equals(DbleServer.COMPLEX_QUERY_EXECUTOR_NAME)) {
                if (decreaseVal-- > 0) {
                    LOGGER.debug("will interrupt thread:{}", thread.toString());
                    thread.interrupt();
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }

    private void increasePoolSize(NameableExecutor nameableExecutor, int increaseVal) {
        DbleServer server = DbleServer.getInstance();
        switch (nameableExecutor.getName()) {
            case DbleServer.BUSINESS_EXECUTOR_NAME:
                for (int i = 0; i < increaseVal; i++) {
                    LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                    if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                        nameableExecutor.execute(new FrontendCurrentRunnable(server.getFrontHandlerQueue(), server.getFrontPriorityQueue()));
                    } else {
                        nameableExecutor.execute(new FrontendBlockRunnable(server.getFrontHandlerQueue(), server.getFrontPriorityQueue()));
                    }
                }
                break;
            case DbleServer.BACKEND_BUSINESS_EXECUTOR_NAME:
                if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                    for (int i = 0; i < increaseVal; i++) {
                        LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                        nameableExecutor.execute(new BackendCurrentRunnable(server.getConcurrentBackHandlerQueue()));
                    }
                }
                break;
            case DbleServer.WRITE_TO_BACKEND_EXECUTOR_NAME:
                for (int i = 0; i < increaseVal; i++) {
                    LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                    nameableExecutor.execute(new WriteToBackendRunnable(server.getWriteToBackendQueue()));
                }
                break;
        }
    }

    private NameableExecutor getExecutor(String executorName) {
        DbleServer server = DbleServer.getInstance();
        switch (executorName) {
            case DbleServer.BUSINESS_EXECUTOR_NAME:
                return (NameableExecutor) server.getBusinessExecutor();
            case DbleServer.BACKEND_BUSINESS_EXECUTOR_NAME:
                return (NameableExecutor) server.getBackendBusinessExecutor();
            case DbleServer.WRITE_TO_BACKEND_EXECUTOR_NAME:
                return (NameableExecutor) server.getWriteToBackendExecutor();
            case DbleServer.COMPLEX_QUERY_EXECUTOR_NAME:
                return (NameableExecutor) server.getComplexQueryExecutor();
        }
        return null;
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        throw new SQLException("not support delete", "42S22", ErrorCode.ER_DUP_ENTRY);
    }
}
