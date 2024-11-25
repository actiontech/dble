/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.btrace.provider.OBsharding_DThreadPoolProvider;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.executor.BackendRunnable;
import com.oceanbase.obsharding_d.net.executor.FrontendRunnable;
import com.oceanbase.obsharding_d.net.executor.ThreadPoolStatistic;
import com.oceanbase.obsharding_d.net.executor.WriteToBackendRunnable;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.services.BackendService;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OBsharding_DThreadPoolTask extends ManagerBaseTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OBsharding_DThreadPoolTask.class);
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_POOL_SIZE = "pool_size";

    public static final String COLUMN_ACTIVE_TASK_COUNT = "active_task_count";

    private static final String COLUMN_TASK_QUEUE_SIZE = "task_queue_size";
    private static final String COLUMN_COMPLETED_TASK_COUNT = "completed_task";
    private static final String COLUMN_TOTAL_TASK_COUNT = "total_task";

    public OBsharding_DThreadPoolTask() {
        super("obsharding-d_thread_pool_task", 6);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(32)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_POOL_SIZE, new ColumnMeta(COLUMN_POOL_SIZE, "int(11)", false));
        columnsType.put(COLUMN_POOL_SIZE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ACTIVE_TASK_COUNT, new ColumnMeta(COLUMN_ACTIVE_TASK_COUNT, "int(11)", false));
        columnsType.put(COLUMN_ACTIVE_TASK_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TASK_QUEUE_SIZE, new ColumnMeta(COLUMN_TASK_QUEUE_SIZE, "int(11)", false));
        columnsType.put(COLUMN_TASK_QUEUE_SIZE, Fields.FIELD_TYPE_LONG);


        columns.put(COLUMN_COMPLETED_TASK_COUNT, new ColumnMeta(COLUMN_COMPLETED_TASK_COUNT, "int(11)", false));
        columnsType.put(COLUMN_COMPLETED_TASK_COUNT, Fields.FIELD_TYPE_LONG);


        columns.put(COLUMN_TOTAL_TASK_COUNT, new ColumnMeta(COLUMN_TOTAL_TASK_COUNT, "int(11)", false));
        columnsType.put(COLUMN_TOTAL_TASK_COUNT, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        OBsharding_DThreadPoolProvider.beginProcessShowThreadPoolTask();
        OBsharding_DServer server = OBsharding_DServer.getInstance();
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);
        lst.add(getRow((NameableExecutor) server.getTimerExecutor()));
        lst.add(getRow((NameableExecutor) server.getFrontExecutor()));
        lst.add(getRow((NameableExecutor) server.getBackendExecutor()));
        lst.add(getRow((NameableExecutor) server.getComplexQueryExecutor()));
        lst.add(getRow((NameableExecutor) server.getWriteToBackendExecutor()));
        return lst;
    }

    private LinkedHashMap<String, String> getRow(NameableExecutor exec) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        final Row rowData = calculateRow(exec);
        row.put(COLUMN_NAME, rowData.getName());
        row.put(COLUMN_POOL_SIZE, rowData.getPoolSize() + "");
        row.put(COLUMN_ACTIVE_TASK_COUNT, rowData.getActiveTaskCount() + "");
        row.put(COLUMN_TASK_QUEUE_SIZE, rowData.getTaskQueueSize() + "");
        row.put(COLUMN_COMPLETED_TASK_COUNT, rowData.getCompletedTask() + "");
        row.put(COLUMN_TOTAL_TASK_COUNT, rowData.getTotalTaskCount() + "");
        return row;
    }

    public static synchronized Row calculateRow(NameableExecutor exec) {
        long activeCount, completedTaskCount, queueSize, totalCount;
        final Map<Thread, Runnable> threadRunnableMap = OBsharding_DServer.getInstance().getRunnableMap().get(exec.getName());
        switch (exec.getName()) {
            case OBsharding_DServer.FRONT_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof FrontendRunnable) && ((FrontendRunnable) ele).getThreadContext().isDoingTask()).count();
                }

                queueSize = OBsharding_DServer.getInstance().getFrontHandlerQueue().size();
                for (IOProcessor frontProcessor : OBsharding_DServer.getInstance().getFrontProcessors()) {
                    for (FrontendConnection con : frontProcessor.getFrontends().values()) {
                        if (con == null) {
                            continue;
                        }
                        final AbstractService service = con.getService();
                        if (service instanceof FrontendService) {
                            queueSize += ((FrontendService<?>) service).getRecvTaskQueueSize();
                        }
                    }
                }
                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getFrontBusiness();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();
                totalCount = activeCount + queueSize + completedTaskCount;
            }
            break;
            case OBsharding_DServer.BACKEND_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof BackendRunnable) && ((BackendRunnable) ele).getThreadContext().isDoingTask()).count();
                }
                if (SystemConfig.getInstance().getUsePerformanceMode() != 1) {
                    queueSize = 0;
                    for (IOProcessor processor : OBsharding_DServer.getInstance().getBackendProcessors()) {
                        for (BackendConnection con : processor.getBackends().values()) {
                            if (con == null) {
                                continue;
                            }
                            final AbstractService service = con.getService();
                            if (service instanceof BackendService) {
                                queueSize += ((BackendService) service).getRecvTaskQueueSize();
                            }
                        }
                    }
                } else {
                    queueSize = OBsharding_DServer.getInstance().getConcurrentBackHandlerQueue().size();
                }

                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getBackendBusiness();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();

                totalCount = activeCount + queueSize + completedTaskCount;

            }
            break;
            case OBsharding_DServer.WRITE_TO_BACKEND_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof WriteToBackendRunnable) && ((WriteToBackendRunnable) ele).getThreadContext().isDoingTask()).count();
                }
                queueSize = OBsharding_DServer.getInstance().getWriteToBackendQueue().size();
                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getWriteToBackend();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();
                totalCount = activeCount + queueSize + completedTaskCount;
            }
            break;

            default:
                activeCount = exec.getActiveCount();
                queueSize = exec.getQueue().size();
                completedTaskCount = exec.getCompletedTaskCount();
                totalCount = exec.getTaskCount();
                break;

        }

        final Row row = new Row();

        row.setName(exec.getName());
        row.setPoolSize(exec.getPoolSize());
        row.setActiveTaskCount(activeCount);
        row.setTaskQueueSize(queueSize);
        row.setCompletedTask(completedTaskCount);
        row.setTotalTaskCount(totalCount);
        return row;


    }


    public static class Row {
        String name;
        long poolSize;
        long activeTaskCount;
        long taskQueueSize;
        long completedTask;
        long totalTaskCount;

        public String getName() {
            return name;
        }

        public Row setName(String nameTmp) {
            name = nameTmp;
            return this;
        }

        public long getPoolSize() {
            return poolSize;
        }

        public Row setPoolSize(long poolSizeTmp) {
            poolSize = poolSizeTmp;
            return this;
        }

        public long getActiveTaskCount() {
            return activeTaskCount;
        }

        public Row setActiveTaskCount(long activeCountTmp) {
            activeTaskCount = activeCountTmp;
            return this;
        }

        public long getTaskQueueSize() {
            return taskQueueSize;
        }

        public Row setTaskQueueSize(long taskQueueSizeTmp) {
            taskQueueSize = taskQueueSizeTmp;
            return this;
        }

        public long getCompletedTask() {
            return completedTask;
        }

        public Row setCompletedTask(long completedTaskTmp) {
            completedTask = completedTaskTmp;
            return this;
        }

        public long getTotalTaskCount() {
            return totalTaskCount;
        }

        public Row setTotalTaskCount(long totalTaskTmp) {
            totalTaskCount = totalTaskTmp;
            return this;
        }
    }
}
