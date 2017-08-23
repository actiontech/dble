package io.mycat.memory;


import com.google.common.annotations.VisibleForTesting;
import io.mycat.config.model.SystemConfig;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.memory.mm.ResultMergeMemoryManager;
import io.mycat.memory.unsafe.storage.DataNodeDiskManager;
import io.mycat.memory.unsafe.storage.SerializerManager;
import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.apache.log4j.Logger;

/**
 * Created by zagnix on 2016/6/2.
 * Mycat内存管理工具类
 * 规划为三部分内存:结果集处理内存,系统预留内存,网络处理内存
 * 其中网络处理内存部分全部为Direct Memory
 * 结果集内存分为Direct Memory 和 Heap Memory，但目前仅使用Direct Memory
 * 系统预留内存为 Heap Memory。
 * 系统运行时，必须设置-XX:MaxDirectMemorySize 和 -Xmx JVM参数
 * -Xmx1024m -Xmn512m -XX:MaxDirectMemorySize=2048m -Xss256K -XX:+UseParallelGC
 */

public class MyCatMemory {
    private static final Logger LOGGER = Logger.getLogger(MyCatMemory.class);

    public static final double DIRECT_SAFETY_FRACTION = 0.7;
    private final long resultSetBufferSize;
    private final int numCores;


    /**
     * 内存管理相关关键类
     */
    private final MycatPropertyConf conf;
    private final MemoryManager resultMergeMemoryManager;
    private final DataNodeDiskManager blockManager;
    private final SerializerManager serializerManager;


    public MyCatMemory(SystemConfig system, long totalNetWorkBufferSize) throws NoSuchFieldException, IllegalAccessException {


        LOGGER.info("useOffHeapForMerge = " + system.getUseOffHeapForMerge());
        LOGGER.info("memoryPageSize = " + system.getMemoryPageSize());
        LOGGER.info("spillsFileBufferSize = " + system.getSpillsFileBufferSize());
        LOGGER.info("totalNetWorkBufferSize = " + JavaUtils.bytesToString2(totalNetWorkBufferSize));
        LOGGER.info("dataNodeSortedTempDir = " + system.getDataNodeSortedTempDir());
        this.conf = new MycatPropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();


        /**
         * 目前merge，order by ，limit 没有使用On Heap内存
         */

        resultSetBufferSize =
                (long) ((Platform.getMaxDirectMemory() - totalNetWorkBufferSize) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;

        /**
         * mycat.merge.memory.offHeap.enabled
         * mycat.buffer.pageSize
         * mycat.memory.offHeap.size
         * mycat.merge.file.buffer
         * mycat.direct.output.result
         * mycat.local.dir
         */

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
        conf.set("server.pointer.array.len", "8k").
                set("server.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize));

        LOGGER.info("resultSetBufferSize: " + JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager =
                new ResultMergeMemoryManager(conf, numCores, 0);


        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true);

    }


    @VisibleForTesting
    public MyCatMemory() throws NoSuchFieldException, IllegalAccessException {
        conf = new MycatPropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();

        long maxOnHeapMemory = (Platform.getMaxHeapMemory());
        assert maxOnHeapMemory > 0;

        resultSetBufferSize = (long) ((Platform.getMaxDirectMemory()) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;
        /**
         * mycat.memory.offHeap.enabled
         * mycat.buffer.pageSize
         * mycat.memory.offHeap.size
         * mycat.testing.memory
         * mycat.merge.file.buffer
         * mycat.direct.output.result
         * mycat.local.dir
         */
        conf.set("server.memory.offHeap.enabled", "true").
                set("server.pointer.array.len", "8K").
                set("server.buffer.pageSize", "1m").
                set("server.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize));

        LOGGER.info("resultSetBufferSize: " + JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager =
                new ResultMergeMemoryManager(conf, numCores, maxOnHeapMemory);

        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true);

    }

    public MycatPropertyConf getConf() {
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
