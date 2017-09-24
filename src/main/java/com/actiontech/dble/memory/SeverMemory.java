/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.memory.unsafe.memory.mm.MemoryManager;
import com.actiontech.dble.memory.unsafe.memory.mm.ResultMergeMemoryManager;
import com.actiontech.dble.memory.unsafe.storage.DataNodeDiskManager;
import com.actiontech.dble.memory.unsafe.storage.SerializerManager;
import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
import com.actiontech.dble.memory.unsafe.utils.ServerPropertyConf;
import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.Logger;

/**
 * Created by zagnix on 2016/6/2. Memory manager used for:result,IO,system IO ia
 * all IO Memory allocation use the Direct Memory
 * result use Direct Memory and Heap
 * System use the Heap Memory.
 * JVM should set -XX:MaxDirectMemorySize and -Xmx  -Xmn
 * -Xss  -XX:+UseParallelGC
 */

public class SeverMemory {
    private static final Logger LOGGER = Logger.getLogger(SeverMemory.class);

    public static final double DIRECT_SAFETY_FRACTION = 0.7;
    private final long resultSetBufferSize;
    private final int numCores;

    private final ServerPropertyConf conf;
    private final MemoryManager resultMergeMemoryManager;
    private final DataNodeDiskManager blockManager;
    private final SerializerManager serializerManager;

    public SeverMemory(SystemConfig system, long totalNetWorkBufferSize)
            throws NoSuchFieldException, IllegalAccessException {

        LOGGER.info("useOffHeapForMerge = " + system.getUseOffHeapForMerge());
        LOGGER.info("memoryPageSize = " + system.getMemoryPageSize());
        LOGGER.info("spillsFileBufferSize = " + system.getSpillsFileBufferSize());
        LOGGER.info("totalNetWorkBufferSize = " + JavaUtils.bytesToString2(totalNetWorkBufferSize));
        LOGGER.info("dataNodeSortedTempDir = " + system.getDataNodeSortedTempDir());
        this.conf = new ServerPropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();

        resultSetBufferSize = (long) ((Platform.getMaxDirectMemory() - totalNetWorkBufferSize) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;


        if (system.getUseOffHeapForMerge() == 1) {
            conf.set("server.memory.offHeap.enabled", "true");
        } else {
            conf.set("server.memory.offHeap.enabled", "false");
        }

        if (system.getMemoryPageSize() != null) {
            conf.set("server.buffer.pageSize", system.getMemoryPageSize());
        } else {
            conf.set("server.buffer.pageSize", "1m");
        }

        if (system.getSpillsFileBufferSize() != null) {
            conf.set("server.merge.file.buffer", system.getSpillsFileBufferSize());
        } else {
            conf.set("server.merge.file.buffer", "32k");
        }

        conf.set("server.local.dirs", system.getDataNodeSortedTempDir());
        conf.set("server.pointer.array.len", "8k").set("server.memory.offHeap.size",
                JavaUtils.bytesToString2(resultSetBufferSize));

        LOGGER.info("resultSetBufferSize: " + JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager = new ResultMergeMemoryManager(conf, numCores, 0);

        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true);

    }

    @VisibleForTesting
    public SeverMemory() throws NoSuchFieldException, IllegalAccessException {
        conf = new ServerPropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();

        long maxOnHeapMemory = (Platform.getMaxHeapMemory());
        assert maxOnHeapMemory > 0;

        resultSetBufferSize = (long) ((Platform.getMaxDirectMemory()) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;
        conf.set("server.memory.offHeap.enabled", "true").set("server.pointer.array.len", "8K").set("server.buffer.pageSize", "1m").set("server.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize));

        LOGGER.info("resultSetBufferSize: " + JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager = new ResultMergeMemoryManager(conf, numCores, maxOnHeapMemory);

        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true);

    }

    public ServerPropertyConf getConf() {
        return conf;
    }

    public long getResultSetBufferSize() {
        return resultSetBufferSize;
    }

    public MemoryManager getResultMergeMemoryManager() {
        return resultMergeMemoryManager;
    }

    public SerializerManager getSerializerManager() {
        return serializerManager;
    }

    public DataNodeDiskManager getBlockManager() {
        return blockManager;
    }

}
