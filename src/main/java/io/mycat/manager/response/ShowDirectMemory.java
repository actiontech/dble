package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * 实现show@@directmemory功能
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-21 17:35
 */

public final class ShowDirectMemory {
    private ShowDirectMemory() {
    }

    private static final int DETAIL_FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket DETAIL_HEADER = PacketUtil.getHeader(DETAIL_FIELD_COUNT);
    private static final FieldPacket[] DETAIL_FIELDS = new FieldPacket[DETAIL_FIELD_COUNT];
    private static final EOFPacket DETAIL_EOF = new EOFPacket();


    private static final int TOTAL_FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket TOTAL_HEADER = PacketUtil.getHeader(TOTAL_FIELD_COUNT);
    private static final FieldPacket[] TOTAL_FIELDS = new FieldPacket[TOTAL_FIELD_COUNT];
    private static final EOFPacket TOTAL_EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        DETAIL_HEADER.packetId = ++packetId;

        DETAIL_FIELDS[i] = PacketUtil.getField("THREAD_ID", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].packetId = ++packetId;

        DETAIL_FIELDS[i] = PacketUtil.getField("MEM_USE_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].packetId = ++packetId;

        DETAIL_FIELDS[i] = PacketUtil.getField("  SIZE  ", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].packetId = ++packetId;
        DETAIL_EOF.packetId = ++packetId;


        i = 0;
        packetId = 0;

        TOTAL_HEADER.packetId = ++packetId;

        TOTAL_FIELDS[i] = PacketUtil.getField("MDIRECT_MEMORY_MAXED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].packetId = ++packetId;

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_USED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].packetId = ++packetId;

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_AVAILABLE", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].packetId = ++packetId;

        TOTAL_FIELDS[i] = PacketUtil.getField("SAFETY_FRACTION", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].packetId = ++packetId;

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_RESERVED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].packetId = ++packetId;
        TOTAL_EOF.packetId = ++packetId;


    }


    public static void execute(ManagerConnection c, int showtype) {

        if (showtype == 1) {
            showDirectMemoryTotal(c);
        } else if (showtype == 2) {
            showDirectMemoryDetail(c);
        }
    }


    public static void showDirectMemoryDetail(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = DETAIL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : DETAIL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = DETAIL_EOF.write(buffer, c, true);

        // write rows
        byte packetId = DETAIL_EOF.packetId;

        int useOffHeapForMerge = MycatServer.getInstance().getConfig().getSystem().getUseOffHeapForMerge();

        ConcurrentMap<Long, Long> networkbufferpool = MycatServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        try {

            if (useOffHeapForMerge == 1) {
                ConcurrentMap<Long, Long> map = MycatServer.getInstance().
                        getServerMemory().
                        getResultMergeMemoryManager().getDirectMemorUsage();
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    RowDataPacket row = new RowDataPacket(DETAIL_FIELD_COUNT);
                    long value = entry.getValue();
                    row.add(String.valueOf(entry.getKey()).getBytes(c.getCharset()));
                    /**
                     * 该DIRECTMEMORY内存被结果集处理使用了
                     */
                    row.add("MergeMemoryPool".getBytes(c.getCharset()));
                    row.add(value > 0 ?
                            JavaUtils.bytesToString2(value).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c, true);
                }
            }

            for (Map.Entry<Long, Long> entry : networkbufferpool.entrySet()) {
                RowDataPacket row = new RowDataPacket(DETAIL_FIELD_COUNT);
                long value = entry.getValue();
                row.add(String.valueOf(entry.getKey()).getBytes(c.getCharset()));
                /**
                 * 该DIRECTMEMORY内存属于Buffer Pool管理的！
                 */
                row.add("NetWorkBufferPool".getBytes(c.getCharset()));
                row.add(value > 0 ?
                        JavaUtils.bytesToString2(value).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));

                row.packetId = ++packetId;
                buffer = row.write(buffer, c, true);
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


    public static void showDirectMemoryTotal(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = TOTAL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : TOTAL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = TOTAL_EOF.write(buffer, c, true);
        // write rows
        byte packetId = TOTAL_EOF.packetId;

        int useOffHeapForMerge = MycatServer.getInstance().getConfig().
                getSystem().getUseOffHeapForMerge();

        ConcurrentMap<Long, Long> networkbufferpool = MycatServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);
        long usedforMerge = 0;
        long usedforNetworkd = 0;

        try {

            /**
             * 通过-XX:MaxDirectMemorySize=2048m设置的值
             */
            row.add(JavaUtils.bytesToString2(Platform.getMaxDirectMemory()).getBytes(c.getCharset()));

            if (useOffHeapForMerge == 1) {

                /**
                 * 结果集合并时，总共消耗的DirectMemory内存
                 */
                ConcurrentMap<Long, Long> concurrentHashMap = MycatServer.getInstance().
                        getServerMemory().
                        getResultMergeMemoryManager().getDirectMemorUsage();
                for (Map.Entry<Long, Long> entry : concurrentHashMap.entrySet()) {
                    usedforMerge += entry.getValue();
                }
            }

            /**
             * 网络packet处理，在buffer pool 已经使用DirectMemory内存
             */
            for (Map.Entry<Long, Long> entry : networkbufferpool.entrySet()) {
                usedforNetworkd += entry.getValue();
            }

            row.add(JavaUtils.bytesToString2(usedforMerge + usedforNetworkd).getBytes(c.getCharset()));


            long totalAvailable = 0;

            if (useOffHeapForMerge == 1) {
                /**
                 * 设置使用off-heap内存处理结果集时，防止客户把MaxDirectMemorySize设置到物理内存的极限。
                 * Mycat能使用的DirectMemory是MaxDirectMemorySize*DIRECT_SAFETY_FRACTION大小，
                 * DIRECT_SAFETY_FRACTION为安全系数，为OS，Heap预留空间，避免因大结果集造成系统物理内存被耗尽！
                 */
                totalAvailable = (long) (Platform.getMaxDirectMemory() * MyCatMemory.DIRECT_SAFETY_FRACTION);
            } else {
                totalAvailable = Platform.getMaxDirectMemory();
            }

            row.add(JavaUtils.bytesToString2(totalAvailable - usedforMerge - usedforNetworkd).getBytes(c.getCharset()));

            if (useOffHeapForMerge == 1) {
                /**
                 * 输出安全系统DIRECT_SAFETY_FRACTION
                 */
                row.add(("" + MyCatMemory.DIRECT_SAFETY_FRACTION).getBytes(c.getCharset()));
            } else {
                row.add(("1.0").getBytes(c.getCharset()));
            }


            long resevedForOs = 0;

            if (useOffHeapForMerge == 1) {
                /**
                 * 预留OS系统部分内存！！！
                 */
                resevedForOs = (long) ((1 - MyCatMemory.DIRECT_SAFETY_FRACTION) *
                        (Platform.getMaxDirectMemory() -
                                2 * MycatServer.getInstance().getTotalNetWorkBufferSize()));
            }

            row.add(resevedForOs > 0 ? JavaUtils.bytesToString2(resevedForOs).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));

            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


}
