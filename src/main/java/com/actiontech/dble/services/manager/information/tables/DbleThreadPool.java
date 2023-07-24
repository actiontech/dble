/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.DbleThreadPoolProvider;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.executor.*;
import com.actiontech.dble.net.impl.nio.RW;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.stream.Collectors;

public final class DbleThreadPool extends ManagerWritableTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbleThreadPool.class);


    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_POOL_SIZE = "pool_size";
    private static final String COLUMN_CORE_POOL_SIZE = "core_pool_size";
    public static final String COLUMN_ACTIVE_COUNT = "active_count";
    private static final String COLUMN_WAITING_TASK_COUNT = "waiting_task_count";

    public DbleThreadPool() {
        super("dble_thread_pool", 5);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(32)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_POOL_SIZE, new ColumnMeta(COLUMN_POOL_SIZE, "int(11)", false));
        columnsType.put(COLUMN_POOL_SIZE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CORE_POOL_SIZE, new ColumnMeta(COLUMN_CORE_POOL_SIZE, "int(11)", false));
        columnsType.put(COLUMN_CORE_POOL_SIZE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ACTIVE_COUNT, new ColumnMeta(COLUMN_ACTIVE_COUNT, "int(11)", false));
        columnsType.put(COLUMN_ACTIVE_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_WAITING_TASK_COUNT, new ColumnMeta(COLUMN_WAITING_TASK_COUNT, "int(11)", false));
        columnsType.put(COLUMN_WAITING_TASK_COUNT, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        DbleServer server = DbleServer.getInstance();
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getTimerExecutor())));
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getFrontExecutor())));
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getManagerFrontExecutor())));
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getBackendExecutor())));
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getComplexQueryExecutor())));
        lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getWriteToBackendExecutor())));
        if (SystemConfig.getInstance().getUsingAIO() == 1) {
            int size = SystemConfig.getInstance().getNIOFrontRW() + SystemConfig.getInstance().getNIOBackendRW();
            lst.add(getRow(new ThreadPoolInfo(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "AIO", size, size, size, 0)));
        } else {
            lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getNioFrontExecutor())));
            lst.add(getRow(new ThreadPoolInfo((NameableExecutor) server.getNioBackendExecutor())));
        }
        return lst;
    }

    private LinkedHashMap<String, String> getRow(ThreadPoolInfo info) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put(COLUMN_NAME, info.getName());
        row.put(COLUMN_POOL_SIZE, info.getPoolSize() + "");
        row.put(COLUMN_CORE_POOL_SIZE, info.getCorePoolSize() + "");
        row.put(COLUMN_ACTIVE_COUNT, info.getActiveCount() + "");
        row.put(COLUMN_WAITING_TASK_COUNT, info.getQueueSize() + "");
        return row;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        throw new SQLException("not support insert", "42S22", ErrorCode.ER_DUP_ENTRY);
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        if (values.containsKey(COLUMN_NAME) || values.containsKey(COLUMN_ACTIVE_COUNT) || values.containsKey(COLUMN_WAITING_TASK_COUNT) || values.containsKey(COLUMN_POOL_SIZE)) {
            throw new SQLException("Column '" + COLUMN_NAME + "/" + COLUMN_ACTIVE_COUNT + "/" + COLUMN_WAITING_TASK_COUNT + "/" + COLUMN_POOL_SIZE + "' is not writable", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
        }
        String corePoolSizeStr = values.get(COLUMN_CORE_POOL_SIZE);
        int corePoolSize;
        if (StringUtil.isBlank(corePoolSizeStr) || (corePoolSize = IntegerUtil.parseInt(corePoolSizeStr)) <= 0) {
            throw new SQLException("Column '" + COLUMN_CORE_POOL_SIZE + "' should be a positive integer greater than 0", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
        }
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            final int oldSize = Integer.parseInt(affectPk.get(COLUMN_CORE_POOL_SIZE));
            NameableExecutor nameableExecutor = getExecutor(affectPk.get(COLUMN_NAME));
            if (null == nameableExecutor) {
                throw new SQLException("the current line does not support modification", "42S22", ErrorCode.ER_DUP_ENTRY);
            }
            nameableExecutor.setCorePoolSize(corePoolSize);
            if (!nameableExecutor.getName().equals(DbleServer.COMPLEX_QUERY_EXECUTOR_NAME)) {
                nameableExecutor.setMaximumPoolSize(corePoolSize);
                if (oldSize < corePoolSize) {
                    try {
                        increasePoolSize(nameableExecutor, corePoolSize - oldSize);
                    } catch (IOException e) {
                        throw new SQLException(e.getMessage(), "42S22", ErrorCode.ER_YES);
                    }
                } else if (oldSize > corePoolSize) {
                    decreasePoolSize(nameableExecutor, oldSize - corePoolSize);
                }
            }
            //persistence
            try {
                WriteDynamicBootstrap.getInstance().changeValue(getConfigParam(affectPk.get(COLUMN_NAME)), corePoolSizeStr);
            } catch (IOException e) {
                throw new SQLException("persistence failed", "42S22", ErrorCode.ER_YES);
            }
        }
        return 0;
    }

    private String getConfigParam(String executorName) {
        switch (executorName) {
            case DbleServer.FRONT_WORKER_NAME:
                return "frontWorker";
            case DbleServer.FRONT_MANAGER_WORKER_NAME:
                return "managerFrontWorker";
            case DbleServer.BACKEND_WORKER_NAME:
                return "backendWorker";
            case DbleServer.WRITE_TO_BACKEND_WORKER_NAME:
                return "writeToBackendWorker";
            case DbleServer.COMPLEX_QUERY_EXECUTOR_NAME:
                return "complexQueryWorker";
            case DbleServer.NIO_FRONT_RW:
                return "NIOFrontRW";
            case DbleServer.NIO_BACKEND_RW:
                return "NIOBackendRW";
            default:
                break;
        }
        return null;
    }

    private void decreasePoolSize(NameableExecutor nameableExecutor, int decreaseVal) {
        Map<Thread, Runnable> runnableMap = DbleServer.getInstance().getRunnableMap().get(nameableExecutor.getName());
        if (nameableExecutor.getName().equals(DbleServer.NIO_FRONT_RW) || nameableExecutor.getName().equals(DbleServer.NIO_BACKEND_RW)) {
            runnableMap = reorderRunnableMap(runnableMap);
        }
        int registerCount = 0;
        Iterator<Map.Entry<Thread, Runnable>> iterator = runnableMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Thread, Runnable> threadRunnableEntry = iterator.next();
            Thread thread = threadRunnableEntry.getKey();
            Runnable runnable = threadRunnableEntry.getValue();
            boolean isNormalBackendExecutor = SystemConfig.getInstance().getUsePerformanceMode() != 1 && nameableExecutor.getName().equals(DbleServer.BACKEND_WORKER_NAME);
            if (runnable != null && !thread.isInterrupted() && !nameableExecutor.getName().equals(DbleServer.COMPLEX_QUERY_EXECUTOR_NAME) && !isNormalBackendExecutor) {
                if (decreaseVal-- > 0 && runnableMap.size() > 1) {
                    LOGGER.debug("will interrupt thread:{}", thread.toString());
                    iterator.remove();
                    if (runnable instanceof RW && ((RW) runnable).getSelectorKeySize() != 0) {
                        //register selector
                        Runnable tailRunnable = getEntryFromTail(runnableMap, ++registerCount);
                        reRegisterSelector(((RW) runnable).getSelector(), tailRunnable);
                    }
                    thread.interrupt();
                } else {
                    break;
                }
            } else if (isNormalBackendExecutor) {
                //short life cycle-the thread does not contain a queue
                DbleServer.getInstance().getThreadUsedMap().remove(thread.getName());
            }
        }
        DbleServer.getInstance().getRunnableMap().put(nameableExecutor.getName(), runnableMap);
    }

    /**
     * Fetch data in reverse order
     *
     * @param runnableMap
     * @param index
     * @return
     */
    private Runnable getEntryFromTail(Map<Thread, Runnable> runnableMap, int index) {
        List<Runnable> runnableList = new ArrayList<>(runnableMap.values());
        int size = runnableList.size();
        return runnableList.get(size >= index ? size - index : index % size);
    }

    /**
     * Sort according to the number of selectorKeys
     *
     * @param runnableMap
     * @return
     */
    private LinkedHashMap<Thread, Runnable> reorderRunnableMap(Map<Thread, Runnable> runnableMap) {
        LinkedHashMap<Thread, Runnable> orderMap = Maps.newLinkedHashMap();
        Map<Thread, Runnable> result = runnableMap.entrySet().stream().sorted((o1, o2) -> {
            RW rw1 = (RW) o1.getValue();
            RW rw2 = (RW) o2.getValue();
            return rw1.getSelectorKeySize() - rw2.getSelectorKeySize();
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldVal, newVal) -> oldVal, LinkedHashMap::new));
        for (Map.Entry<Thread, Runnable> threadRunnableEntry : result.entrySet()) {
            orderMap.put(threadRunnableEntry.getKey(), threadRunnableEntry.getValue());
        }
        return orderMap;
    }

    /**
     * Re-register the channel to the selector
     *
     * @param selector
     * @param runnable
     */
    private void reRegisterSelector(Selector selector, Runnable runnable) {
        if (null == runnable) {
            LOGGER.warn("register error.");
        }
        RW rw = (RW) runnable;
        // loop over all the keys that are registered with the old Selector
        // and register them with the new one
        ArrayList<SelectionKey> selectionKeys = Lists.newArrayList(selector.keys());
        for (SelectionKey key : selectionKeys) {
            if (!key.isValid()) {
                continue;
            }
            SelectableChannel ch = key.channel();
            int ops = key.interestOps();
            Object att = key.attachment();
            AbstractConnection connection = (AbstractConnection) att;
            LOGGER.debug("register connection:{},rw:{}", connection.getId(), rw.toString());
            // cancel the old key
            key.cancel();

            try {
                DbleThreadPoolProvider.reRegisterSelector();
                //wake up select-avoid lock time-consuming
                rw.getSelector().wakeup();
                // register the channel with the new selector now
                ch.register(rw.getSelector(), ops, att);
            } catch (ClosedChannelException e) {
                // close channel
                try {
                    ch.close();
                } catch (IOException ioException) {
                    LOGGER.warn("error:", e);
                }
            }
        }
    }

    private void increasePoolSize(NameableExecutor nameableExecutor, int increaseVal) throws IOException {
        DbleServer server = DbleServer.getInstance();
        switch (nameableExecutor.getName()) {
            case DbleServer.FRONT_WORKER_NAME:
                for (int i = 0; i < increaseVal; i++) {
                    LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                    if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                        nameableExecutor.execute(new FrontendCurrentRunnable(server.getFrontHandlerQueue()));
                    } else {
                        nameableExecutor.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) server.getFrontHandlerQueue()));
                    }
                }
                break;
            case DbleServer.FRONT_MANAGER_WORKER_NAME:
                for (int i = 0; i < increaseVal; i++) {
                    LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                    nameableExecutor.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) server.getManagerFrontHandlerQueue()));
                }
                break;
            case DbleServer.BACKEND_WORKER_NAME:
                if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                    for (int i = 0; i < increaseVal; i++) {
                        LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                        nameableExecutor.execute(new BackendCurrentRunnable(server.getConcurrentBackHandlerQueue()));
                    }
                }
                break;
            case DbleServer.WRITE_TO_BACKEND_WORKER_NAME:
                for (int i = 0; i < increaseVal; i++) {
                    LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                    nameableExecutor.execute(new WriteToBackendRunnable(server.getWriteToBackendQueue()));
                }
                break;
            case DbleServer.NIO_FRONT_RW:
                if (SystemConfig.getInstance().getUsingAIO() != 1) {
                    for (int i = 0; i < increaseVal; i++) {
                        LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                        nameableExecutor.execute(new RW(server.getFrontRegisterQueue()));
                    }
                }
                break;
            case DbleServer.NIO_BACKEND_RW:
                if (SystemConfig.getInstance().getUsingAIO() != 1) {
                    for (int i = 0; i < increaseVal; i++) {
                        LOGGER.debug("will execute thread:{}", nameableExecutor.toString());
                        nameableExecutor.execute(new RW(server.getBackendRegisterQueue()));
                    }
                }
                break;
            default:
                break;
        }
    }

    private NameableExecutor getExecutor(String executorName) {
        DbleServer server = DbleServer.getInstance();
        switch (executorName) {
            case DbleServer.FRONT_WORKER_NAME:
                return (NameableExecutor) server.getFrontExecutor();
            case DbleServer.FRONT_MANAGER_WORKER_NAME:
                return (NameableExecutor) server.getManagerFrontExecutor();
            case DbleServer.BACKEND_WORKER_NAME:
                return (NameableExecutor) server.getBackendExecutor();
            case DbleServer.WRITE_TO_BACKEND_WORKER_NAME:
                return (NameableExecutor) server.getWriteToBackendExecutor();
            case DbleServer.COMPLEX_QUERY_EXECUTOR_NAME:
                return (NameableExecutor) server.getComplexQueryExecutor();
            case DbleServer.NIO_FRONT_RW:
                return (NameableExecutor) server.getNioFrontExecutor();
            case DbleServer.NIO_BACKEND_RW:
                return (NameableExecutor) server.getNioBackendExecutor();
            default:
                break;
        }
        return null;
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        throw new SQLException("not support delete", "42S22", ErrorCode.ER_DUP_ENTRY);
    }


    static class ThreadPoolInfo {
        private final String name;
        private final int poolSize;
        private final int corePoolSize;
        private final int activeCount;
        private final int queueSize;

        ThreadPoolInfo(NameableExecutor nameableExecutor) {
            this.name = nameableExecutor.getName();
            this.poolSize = nameableExecutor.getPoolSize();
            this.corePoolSize = nameableExecutor.getCorePoolSize();
            this.activeCount = nameableExecutor.getActiveCount();
            this.queueSize = nameableExecutor.getQueue().size();
        }

        ThreadPoolInfo(String name, int poolSize, int corePoolSize, int activeCount, int queueSize) {
            this.name = name;
            this.poolSize = poolSize;
            this.corePoolSize = corePoolSize;
            this.activeCount = activeCount;
            this.queueSize = queueSize;
        }

        public String getName() {
            return name;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public int getActiveCount() {
            return activeCount;
        }

        public int getQueueSize() {
            return queueSize;
        }
    }
}
