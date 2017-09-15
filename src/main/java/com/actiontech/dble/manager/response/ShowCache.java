/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheService;
import com.actiontech.dble.cache.CacheStatic;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

public final class ShowCache {
    private ShowCache() {
    }

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CACHE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CUR", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("ACCESS", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("HIT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("PUT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_ACCESS", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_PUT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.getPacketId();
        CacheService cacheService = DbleServer.getInstance().getCacheService();
        for (Map.Entry<String, CachePool> entry : cacheService.getAllCachePools().entrySet()) {
            String cacheName = entry.getKey();
            CachePool cachePool = entry.getValue();
            if (cachePool != null) {
                if (cachePool instanceof LayerCachePool) {
                    for (Map.Entry<String, CacheStatic> staticsEntry : ((LayerCachePool) cachePool).getAllCacheStatic().entrySet()) {
                        RowDataPacket row = getRow(cacheName + '.' + staticsEntry.getKey(), staticsEntry.getValue(), c.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, c, true);
                    }
                } else {
                    RowDataPacket row = getRow(cacheName, cachePool.getCacheStatic(), c.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String poolName,
                                        CacheStatic cacheStatic, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(poolName, charset));
        // max size
        row.add(LongUtil.toBytes(cacheStatic.getMaxSize()));
        row.add(LongUtil.toBytes(cacheStatic.getItemSize()));
        row.add(LongUtil.toBytes(cacheStatic.getAccessTimes()));
        row.add(LongUtil.toBytes(cacheStatic.getHitTimes()));
        row.add(LongUtil.toBytes(cacheStatic.getPutTimes()));
        row.add(StringUtil.encode(FormatUtil.formatDate(cacheStatic.getLastAccesTime()), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(cacheStatic.getLastPutTime()), charset));
        return row;
    }

}
