package io.mycat.memory.unsafe.memory.mm;


import io.mycat.memory.unsafe.utils.MycatPropertyConf;

/**
 * Created by zagnix on 2016/6/7.
 */
public class ResultMergeMemoryManager extends MemoryManager {

    public ResultMergeMemoryManager(MycatPropertyConf conf, int numCores, long onHeapExecutionMemory) {
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
