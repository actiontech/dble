/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * ShowHelp
 *
 * @author mycat
 * @author mycat
 */
public final class ShowHelp {
    private ShowHelp() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("STATEMENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
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
        for (String key : KEYS) {
            RowDataPacket row = getRow(key, HELPS.get(key), c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(String stmt, String desc, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(stmt, charset));
        row.add(StringUtil.encode(desc, charset));
        return row;
    }

    private static final Map<String, String> HELPS = new LinkedHashMap<>();
    private static final List<String> KEYS = new LinkedList<>();

    static {

        //select
        HELPS.put("select @@VERSION_COMMENT;", "Show the version comment of dble");
        // show
        HELPS.put("show @@time.current", "Report current timestamp");
        HELPS.put("show @@time.startup", "Report startup timestamp");
        HELPS.put("show @@version", "Report Server version");
        HELPS.put("show @@server", "Report server status");
        HELPS.put("show @@threadpool", "Report threadPool status");
        HELPS.put("show @@database", "Report databases");
        HELPS.put("show @@datanode [where schema = ?]", "Report dataNodes");
        HELPS.put("show @@datasource [where dataNode = ?]", "Report dataSources");
        HELPS.put("show @@datasource.synstatus", "Report datasource data synchronous");
        HELPS.put("show @@datasource.syndetail where name=?", "Report datasource data synchronous detail");
        HELPS.put("show @@datasource.cluster", "Report datasource galary cluster variables");
        HELPS.put("show @@processor", "Report processor status");
        HELPS.put("show @@command", "Report commands status");
        HELPS.put("show @@connection", "Report connection status");
        HELPS.put("show @@cache", "Report system cache usage");
        HELPS.put("show @@backend", "Report backend connection status");
        HELPS.put("show @@session", "Report front session details");
        HELPS.put("show @@connection.sql", "Report connection sql");
        HELPS.put("show @@sql", "Report SQL list");
        // helps.put("show @@sql where id = ?", "Report  specify SQL");
        HELPS.put("show @@sql.high", "Report Hight Frequency SQL");
        HELPS.put("show @@sql.slow", "Report slow SQL");
        HELPS.put("show @@sql.large", "Report the sql witch resultset larger than 10000 rows");
        HELPS.put("show @@sql.condition", "Report the query of a specific table.column set by reload query_cf");
        HELPS.put("show @@sql.resultset", "Report BIG RESULTSET SQL");
        HELPS.put("show @@sql.sum", "Report  User RW Stat ");
        HELPS.put("show @@sql.sum.user", "Report  User RW Stat ");
        HELPS.put("show @@sql.sum.table", "Report  Table RW Stat ");
        HELPS.put("show @@heartbeat", "Report heartbeat status");
        HELPS.put("show @@heartbeat.detail where name=?", "Report heartbeat current detail");
        HELPS.put("show @@sysparam", "Report system param");
        HELPS.put("show @@syslog limit=?", "Report system log");
        HELPS.put("show @@white", "Report server white host ");
        HELPS.put("show @@directmemory=1 or 2", "Report server direct memory usage");
        HELPS.put("show @@command.count", "Report the current number of querys");
        HELPS.put("show @@connection.count", "Report the current number of connections");
        HELPS.put("show @@backend.statistics", "Report backend node info");
        HELPS.put("show @@backend.old", "Report old connections witch still alive after reload config all");
        HELPS.put("show @@binlog.status", "Report the current GTID of all backend nodes");
        HELPS.put("show @@help", "Report usage of manager port");

        HELPS.put("show @@cost_time", "Report cost time of query , contains back End ,front End and over all");
        HELPS.put("show @@thread_used", "Report usage of threads, for optimize performance ");
        HELPS.put("show @@dataNodes where schema=? and table=?", "Report the data nodes info of a table");
        HELPS.put("show @@algorithm where schema=? and table=?", "Report the algorithm info of a table");
        // switch
        HELPS.put("switch @@datasource name:index", "Switch dataSource");

        // kill
        HELPS.put("kill @@connection id1,id2,...", "Kill the specified connections");

        // stop
        HELPS.put("stop @@heartbeat name:time", "Pause dataNode heartbeat");

        // reload
        HELPS.put("reload @@config", "Reload basic config from file");
        HELPS.put("reload @@config_all", "Reload all config from file");
        HELPS.put("reload @@metadata", "Reload metadata of tables");
        HELPS.put("reload @@sqlslow=", "Set Slow SQL Time(ms)");
        HELPS.put("reload @@user_stat", "Reset show @@sql  @@sql.sum @@sql.slow");
        HELPS.put("reload @@query_cf[=table&column]", "Reset show @@sql.conditiont");
        // rollback
        HELPS.put("rollback @@config", "Rollback all config from memory");

        // offline/online
        HELPS.put("offline", "Change Server status to OFF");
        HELPS.put("online", "Change Server status to ON");

        HELPS.put("show @@thread_used", "Report all bussiness&reactor thread usage");

        //file
        HELPS.put("file @@list", "List all the file in conf directory");
        HELPS.put("file @@show filename", "Show the file data of specific file");
        HELPS.put("file @@upload filename content", "Write content to file");

        //log
        HELPS.put("log @@[file=? limit=? key=? regex=?]", "Report logs by given regex");

        // list sort
        KEYS.addAll(HELPS.keySet());
    }

}
