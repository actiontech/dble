/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.DbleThreadPoolProvider;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.executor.BackendRunnable;
import com.actiontech.dble.net.executor.FrontendRunnable;
import com.actiontech.dble.net.executor.ThreadPoolStatistic;
import com.actiontech.dble.net.executor.WriteToBackendRunnable;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.BackendService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.NameableScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.actiontech.dble.DbleServer.TIMER_SCHEDULER_WORKER_NAME;

public final class DbleThreadPoolTask extends ManagerBaseTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbleThreadPoolTask.class);
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_POOL_SIZE = "pool_size";

    public static final String COLUMN_ACTIVE_TASK_COUNT = "active_task_count";

    private static final String COLUMN_TASK_QUEUE_SIZE = "task_queue_size";
    private static final String COLUMN_COMPLETED_TASK_COUNT = "completed_task";
    private static final String COLUMN_TOTAL_TASK_COUNT = "total_task";

    public DbleThreadPoolTask() {
        super("dble_thread_pool_task", 6);
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
        DbleThreadPoolProvider.beginProcessShowThreadPoolTask();
        DbleServer server = DbleServer.getInstance();
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);
        lst.add(getRow((NameableExecutor) server.getTimerExecutor()));
        lst.add(getRow(server.getTimerSchedulerExecutor()));
        lst.add(getRow((NameableExecutor) server.getFrontExecutor()));
        lst.add(getRow((NameableExecutor) server.getManagerFrontExecutor()));
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

    private LinkedHashMap<String, String> getRow(NameableScheduledThreadPoolExecutor exec) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        if (exec.getName().equals(TIMER_SCHEDULER_WORKER_NAME)) {
            row.put(COLUMN_NAME, exec.getName());
            row.put(COLUMN_POOL_SIZE, exec.getPoolSize() + "");
            row.put(COLUMN_ACTIVE_TASK_COUNT, exec.getActiveCount() + "");
            row.put(COLUMN_TASK_QUEUE_SIZE, exec.getQueue().size() + "");
            row.put(COLUMN_COMPLETED_TASK_COUNT, exec.getCompletedTaskCount() + "");
            row.put(COLUMN_TOTAL_TASK_COUNT, exec.getTaskCount() + "");
            return row;
        }
        return null;
    }

    public static synchronized Row calculateRow(NameableExecutor exec) {
        long activeCount, completedTaskCount, queueSize, totalCount;
        final Map<Thread, Runnable> threadRunnableMap = DbleServer.getInstance().getRunnableMap().get(exec.getName());
        switch (exec.getName()) {
            case DbleServer.FRONT_WORKER_NAME:
            case DbleServer.FRONT_MANAGER_WORKER_NAME:
                return calculateRowForFront(exec, threadRunnableMap);
            case DbleServer.BACKEND_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof BackendRunnable) && ((BackendRunnable) ele).getThreadContext().isDoingTask()).count();
                }
                if (SystemConfig.getInstance().getUsePerformanceMode() != 1) {
                    queueSize = 0;
                    for (IOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
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
                    queueSize = DbleServer.getInstance().getConcurrentBackHandlerQueue().size();
                }

                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getBackendBusiness();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();

                totalCount = activeCount + queueSize + completedTaskCount;

            }
            break;
            case DbleServer.WRITE_TO_BACKEND_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof WriteToBackendRunnable) && ((WriteToBackendRunnable) ele).getThreadContext().isDoingTask()).count();
                }
                queueSize = DbleServer.getInstance().getWriteToBackendQueue().size();
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

    public static Row calculateRowForFront(NameableExecutor exec, final Map<Thread, Runnable> threadRunnableMap) {
        long activeCount = 0, completedTaskCount = 0, queueSize = 0, totalCount = 0;
        switch (exec.getName()) {
            case DbleServer.FRONT_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof FrontendRunnable) && ((FrontendRunnable) ele).getThreadContext().isDoingTask()).count();
                }

                queueSize = DbleServer.getInstance().getFrontHandlerQueue().size();
                for (IOProcessor frontProcessor : DbleServer.getInstance().getFrontProcessors()) {
                    for (FrontendConnection con : frontProcessor.getFrontends().values()) {
                        if (con == null) {
                            continue;
                        }
                        final AbstractService service = con.getService();
                        if (!con.isManager() && service instanceof FrontendService) {
                            queueSize += ((FrontendService<?>) service).getRecvTaskQueueSize();
                        }
                    }
                }
                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getFrontBusiness();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();
                totalCount = activeCount + queueSize + completedTaskCount;
            }
            break;
            case DbleServer.FRONT_MANAGER_WORKER_NAME: {
                if (threadRunnableMap == null) {
                    activeCount = 0;
                } else {
                    activeCount = threadRunnableMap.values().stream().filter(ele -> (ele instanceof FrontendRunnable) && ((FrontendRunnable) ele).getThreadContext().isDoingTask()).count();
                }

                queueSize = DbleServer.getInstance().getManagerFrontHandlerQueue().size();
                for (IOProcessor frontProcessor : DbleServer.getInstance().getFrontProcessors()) {
                    for (FrontendConnection con : frontProcessor.getFrontends().values()) {
                        if (con == null) {
                            continue;
                        }
                        final AbstractService service = con.getService();
                        if (con.isManager() && service instanceof FrontendService) {
                            queueSize += ((FrontendService<?>) service).getRecvTaskQueueSize();
                        }
                    }
                }
                final ThreadPoolStatistic statistic = ThreadPoolStatistic.getFrontManager();
                completedTaskCount = statistic.getCompletedTaskCount().longValue();
                totalCount = activeCount + queueSize + completedTaskCount;
            }
            break;
            default:
                break;
        }
        Row row = new Row();
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
