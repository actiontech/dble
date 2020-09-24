/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
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
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        for (String key : KEYS) {
            RowDataPacket row = getRow(key, HELPS.get(key), service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
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
        HELPS.put("show @@shardingNode [where schema = ?]", "Report shardingNodes");
        HELPS.put("show @@dbinstance [where shardingNode = ?]", "Report dbinstance");
        HELPS.put("show @@dbinstance.synstatus", "Report dbinstance data synchronous");
        HELPS.put("show @@dbinstance.syndetail where name=?", "Report dbinstance data synchronous detail");
        HELPS.put("show @@processor", "Report processor status");
        HELPS.put("show @@command", "Report commands status");
        HELPS.put("show @@connection where processor=? and front_id=? and host=? and user=?", "Report connection status");
        HELPS.put("show @@cache", "Report system cache usage");
        HELPS.put("show @@backend where processor=? and backend_id=? and mysql_id=? and host=? and port=?", "Report backend connection status");
        HELPS.put("show @@session", "Report front session details");
        HELPS.put("show @@session.xa", "Report front session and associated xa transaction details");
        HELPS.put("show @@connection.sql", "Report connection sql");
        HELPS.put("show @@connection.sql.status where FRONT_ID= ?;", "Show current connection sql status and detail");
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
        HELPS.put("show @@directmemory", "Report server direct memory pool usage");
        HELPS.put("show @@command.count", "Report the current number of querys");
        HELPS.put("show @@connection.count", "Report the current number of connections");
        HELPS.put("show @@backend.statistics", "Report backend node info");
        HELPS.put("show @@backend.old", "Report old connections witch still alive after reload config all");
        HELPS.put("show @@binlog.status", "Report the current GTID of all backend nodes");
        HELPS.put("show @@help", "Report usage of manager port");
        HELPS.put("show @@processlist", "Report correspondence between front and backend session");

        HELPS.put("show @@cost_time", "Report cost time of query , contains back End ,front End and over all");
        HELPS.put("show @@thread_used", "Report usage of threads, for optimize performance ");
        HELPS.put("show @@shardingNodes where schema='?' and table='?'", "Report the sharding nodes info of a table");
        HELPS.put("show @@algorithm where schema='?' and table='?'", "Report the algorithm info of a table");
        HELPS.put("show @@ddl", "Report all ddl info in progress");
        HELPS.put("show @@reload_status", "Report latest reload status in this dble");
        HELPS.put("show @@user", "Report all user in this dble");
        HELPS.put("show @@user.privilege", "Report privilege of all business user in this dble");
        HELPS.put("show @@questions", "Report the questions & transactions have been executed in server port");
        HELPS.put("show @@data_distribution where table ='schema.table'", "Report the data distribution in different sharding node");
        HELPS.put("show @@connection_pool", "Report properties of connection pool");

        // kill
        HELPS.put("kill @@connection id1,id2,...", "Kill the specified connections");
        HELPS.put("kill @@xa_session id1,id2,...", "Kill the specified sessions that commit/rollback xa transaction in the background");
        HELPS.put("kill @@ddl_lock where schema='?' and table='?'", "Kill ddl lock held by the specified ddl");

        // stop
        HELPS.put("stop @@heartbeat name:time", "Pause shardingNode heartbeat");

        // reload
        HELPS.put("reload @@config", "Reload basic config from file");
        HELPS.put("reload @@config_all", "Reload all config from file");
        HELPS.put("reload @@metadata [where schema=? [and table=?] | where table in ('schema1.table1',...)]", "Reload metadata of tables or specified table");
        HELPS.put("reload @@sqlslow=", "Set Slow SQL Time(ms)");
        HELPS.put("reload @@user_stat", "Reset show @@sql  @@sql.sum @@sql.slow");
        HELPS.put("reload @@query_cf[=table&column]", "Reset show @@sql.conditiont");
        HELPS.put("release @@reload_metadata", "Release reload process , unlock the config meta lock");

        // offline/online
        HELPS.put("offline", "Change Server status to OFF");
        HELPS.put("online", "Change Server status to ON");

        HELPS.put("show @@thread_used", "Report all bussiness&reactor thread usage");

        //file
        HELPS.put("file @@list", "List all the file in conf directory");
        HELPS.put("file @@show filename", "Show the file data of specific file");
        HELPS.put("file @@upload filename content", "Write content to file");
        HELPS.put("flow_control @@show", "Show the current config of the flow control");
        HELPS.put("flow_control @@list", "List all the connection be flow-control now");
        HELPS.put("flow_control @@set [enableFlowControl = true/false] [flowControlStart = ?] [flowControlEnd = ?] ", "Change the config of flow control");

        //log
        HELPS.put("log @@[file=? limit=? key=? regex=?]", "Report logs by given regex");

        //dryrun
        HELPS.put("dryrun", "Dry run to check config before reload xml");

        //pause
        HELPS.put("pause @@shardingNode = 'dn1,dn2,....' and timeout = ? [,queue = ?,wait_limit = ?]", "Block query requests witch specified shardingNodes involved ");
        HELPS.put("RESUME", "Resume the query requests of the paused shardingNodes");
        HELPS.put("show @@pause", "Show which shardingNodes have bean pause");

        //slow_query_log
        HELPS.put("show @@slow_query_log", "Show if the slow query log is enabled");
        HELPS.put("enable @@slow_query_log", "Turn on the slow query log");
        HELPS.put("disable @@slow_query_log", "Turn off the slow query log");
        HELPS.put("show @@slow_query.time", "Show the threshold of slow sql, the unit is millisecond");
        HELPS.put("reload @@slow_query.time", "Reset the threshold of slow sql");
        HELPS.put("show @@slow_query.flushperiod", "Show the min flush period for writing to disk");
        HELPS.put("reload @@slow_query.flushperiod", "Reset the flush period");
        HELPS.put("show @@slow_query.flushsize", "Show the min flush size for writing to disk");
        HELPS.put("reload @@slow_query.flushsize", "Reset the flush size");

        //create database
        HELPS.put("create database @@shardingNode ='dn......'", "create database for shardingNode in config");
        //drop database
        HELPS.put("drop database @@shardingNode ='dn......'", "drop database for shardingNode set in config");

        //check @@metadata
        HELPS.put("check @@metadata", "show last time of `reload @@metadata`/start dble");
        HELPS.put("check @@global (schema = '?'( and table = '?'))", "check global and get check result immediately");
        HELPS.put("check full @@metadata", "show detail information of metadata");

        //alert
        HELPS.put("show @@alert", "Show if the alert is enabled");
        HELPS.put("enable @@alert", "Turn on the alert");
        HELPS.put("disable @@alert", "Turn off the alert");

        //ha
        HELPS.put("dbGroup @@disable name='?' (instance = '?')", "disable some dbGroup/dbInstance");
        HELPS.put("dbGroup @@enable name='?' (instance = '?')", "enable some dbGroup/dbInstance");
        HELPS.put("dbGroup @@switch name='?' master='?'", "switch primary in one dbGroup");
        HELPS.put("dbGroup @@events ", "show all the dbGroup ha event which not finished yet ");

        //dump file
        HELPS.put("split src dest -sschema -r500 -w500 -l10000 --ignore", "split dump file into multi dump files according to shardingNode");

        // fresh con
        HELPS.put("fresh conn [forced] where dbGroup ='?' [and dbInstance ='?']", "fresh conn some dbGroup/dbInstance");

        // cap_client_found_rows
        HELPS.put("show @@cap_client_found_rows", "Show if the clientFoundRows capabilities is enabled");
        HELPS.put("enable @@cap_client_found_rows", "Turn on the clientFoundRows capabilities");
        HELPS.put("disable @@cap_client_found_rows", "Turn off the clientFoundRows capabilities");

        // list sort
        KEYS.addAll(HELPS.keySet());
    }

}
