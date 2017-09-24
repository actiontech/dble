/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.memory.mm;


import com.actiontech.dble.memory.unsafe.utils.ServerPropertyConf;

/**
 * Created by zagnix on 2016/6/7.
 */
public class ResultMergeMemoryManager extends MemoryManager {

    public ResultMergeMemoryManager(ServerPropertyConf conf, int numCores, long onHeapExecutionMemory) {
        super(conf, numCores, onHeapExecutionMemory);
    }

    @Override
    protected synchronized long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) throws InterruptedException {
        if (memoryMode == MemoryMode.ON_HEAP) {
            return onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId);
        } else if (memoryMode == MemoryMode.OFF_HEAP) {
            return offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId);
        }
        return 0L;
    }

}
