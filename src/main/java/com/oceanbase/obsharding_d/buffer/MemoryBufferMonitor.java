/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.buffer;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * @author dcy
 * Create Date: 2022-10-14
 */
public class MemoryBufferMonitor {
    private static final Logger LOGGER = LogManager.getLogger(MemoryBufferMonitor.class);
    static final MemoryBufferMonitor INSTANCE = new MemoryBufferMonitor();
    final Map<Long/*address*/, BufferPoolRecord> monitorMap = new ConcurrentHashMap<>();

    private volatile boolean enable = false;
    static final int TRACE_LINE_NUM = 8;

    public MemoryBufferMonitor() {
        this.enable = SystemConfig.getInstance().getEnableMemoryBufferMonitor() == 1;
    }

    public void setEnable(boolean enable) {
        if (!enable) {
            clean();
        }
        this.enable = enable;
    }

    private void clean() {
        monitorMap.clear();
    }

    public boolean isEnable() {
        return enable;
    }

    public static MemoryBufferMonitor getInstance() {
        return INSTANCE;
    }

    public void recordForEach(BiConsumer<? super Long, ? super BufferPoolRecord> action) {
        monitorMap.forEach(action);
    }

    public void remove(Long allocateAddress) {
        if (!enable) {
            return;
        }
        final BufferPoolRecord record = monitorMap.remove(allocateAddress);
        if (record != null) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("removed  buffer record ,address: {}, content:{}", allocateAddress, record);
        }
    }

    public void addRecord(BufferPoolRecord.Builder bufferRecordBuilder, long allocateAddress, int allocateSize) {
        if (!enable) {
            return;
        }
        try {
            if (bufferRecordBuilder == null) {
                bufferRecordBuilder = BufferPoolRecord.builder();
            }
            if (SystemConfig.getInstance().getEnableMemoryBufferMonitorRecordPool() == 0 && bufferRecordBuilder.getType() == BufferType.POOL) {
                return;
            }

            try {
                throw new Exception("for debug");
            } catch (Exception e) {
                String[] stackTrace;
                final StackTraceElement[] fromStackTrace = e.getStackTrace();

                int len = Math.min(fromStackTrace.length, TRACE_LINE_NUM);
                stackTrace = new String[len];
                for (int i = 0; i < len; i++) {
                    stackTrace[i] = fromStackTrace[i].toString();
                }
                bufferRecordBuilder.withStacktrace(stackTrace);

            }
            final BufferPoolRecord record = bufferRecordBuilder.withAllocatedTime(System.currentTimeMillis()).withAllocateSize(allocateSize).build();
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("new  buffer record ,address: {}, content:{}", allocateAddress, record);
            monitorMap.put(allocateAddress, record);

        } catch (Exception e) {
            LOGGER.warn("record buffer monitor error", e);
        } finally {
            if (!enable) {
                clean();
            }
        }
    }


}
