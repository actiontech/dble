/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CheckFullMetaData {
    /* check full @@metadata
    check full @@metadata where sharding = 'xx'
    check full @@metadata where sharding = 'xx' and table ='x'
    check full @@metadata where reload_time = '2018-01-05 11:01:04'
    check full @@metadata where reload_time >= '2018-01-05 11:01:04'
    check full @@metadata where reload_time <= '2018-01-05 11:01:04'
    check full @@metadata where reload_time is null
    check full @@metadata where consistent_in_sharding_nodes=0
    check full @@metadata where consistent_in_sharding_nodes = 1
    check full @@metadata where consistent_in_memory=0
    check full @@metadata where consistent_in_memory = 1
    */
    private static final Pattern PATTERN = Pattern.compile("^\\s*(check\\s+full\\s+@@metadata)(\\s+where\\s+" +
            "((schema\\s*=\\s*" +
            "(('|\")((?!`)((?!\\6).))+\\6|[a-zA-Z_0-9\\-]+)" +
            "(\\s+and\\s+table\\s*=\\s*" +
            "(('|\")((?!`)((?!(\\11)).))+(\\11)|[a-zA-Z_0-9\\-]+))?)" +
            "|(reload_time\\s*([><])?=\\s*(['\"])([0-9:\\-\\s]+)(['\"]))" +
            "|(reload_time\\s+is\\s+null)" +
            "|((consistent_in_sharding_nodes|consistent_in_memory)\\s*=\\s*([01]))))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("schema", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("table", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("reload_time", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("table_structure", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("consistent_in_sharding_nodes", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("consistent_in_memory", Fields.FIELD_TYPE_LONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private CheckFullMetaData() {
    }

    public static void execute(ManagerService service, String stmt) {
        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches()) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql does not match: check full @@metadata [ where [schema='testSchema' [and table ='testTable']] | [reload_time [>|<]= 'yyyy-MM-dd HH:mm:ss']] | [reload_time is null] | [consistent_in_sharding_nodes =0|1] | [consistent_in_memory =0|1]");
            return;
        }
        final ReentrantLock lock = ProxyMeta.getInstance().getTmManager().getMetaLock();
        lock.lock();
        List<RowDataPacket> rows;
        try {
            if (ma.group(2) != null) {
                //  filter
                if (ma.group(4) != null) {
                    String schema = StringUtil.removeAllApostrophe(ma.group(5));
                    if (DbleServer.getInstance().getConfig().getSchemas().get(schema) == null) {
                        service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The schema [" + schema + "] doesn't exist");
                        return;
                    }

                    if (ma.group(9) != null) {
                        String table = StringUtil.removeAllApostrophe(ma.group(10));
                        if (DbleServer.getInstance().getConfig().getSchemas().get(schema).getTables().get(table) == null &&
                                (ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema) == null ||
                                        ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getTableMeta(table) == null)) {
                            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + schema + "." + table + "] doesn't exist");
                            return;
                        }
                        rows = getTableRows(schema, table, service.getCharset().getResults());
                    } else {
                        rows = getSchemaRows(schema, service.getCharset().getResults());
                    }
                } else if (ma.group(16) != null) {
                    String cmpOperator = ma.group(17);
                    String dateFilter = ma.group(19);
                    long timeToCmp;
                    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    try {
                        timeToCmp = sdf.parse(dateFilter).getTime() / 1000;
                    } catch (ParseException e) {
                        service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The format of reload_time in sql must be yyyy-MM-dd HH:mm:ss");
                        return;
                    }
                    rows = getCmpRows(cmpOperator, timeToCmp, service.getCharset().getResults());
                } else if (ma.group(21) != null) {
                    rows = getAllNullRows(service.getCharset().getResults());
                } else { //if (ma.group(18) != null)
                    String filterKey = ma.group(23);
                    String filterValue = ma.group(24);
                    rows = getConsistentRows(filterKey, filterValue, service.getCharset().getResults());
                }
            } else {
                rows = getAllRows(service.getCharset().getResults());
            }
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "check full @@metadata failed, There is other session is doing reload");
            return;
        } finally {
            lock.unlock();
        }

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
        for (RowDataPacket row : rows) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getCmpRows(String cmpOperator, long toCmp, String charset) {
        List<RowDataPacket> list = new ArrayList<>();
        for (Map.Entry<String, SchemaMeta> schemaMetaEntry : ProxyMeta.getInstance().getTmManager().getCatalogs().entrySet()) {
            String schemaName = schemaMetaEntry.getKey();
            SchemaMeta schemaMeta = schemaMetaEntry.getValue();
            for (Map.Entry<String, TableMeta> tableMetaEntry : schemaMeta.getTableMetas().entrySet()) {
                String tableName = tableMetaEntry.getKey();
                TableMeta tableMeta = tableMetaEntry.getValue();
                long timeStamp = tableMeta.getVersion();
                String createQuery = tableMeta.getCreateSql();
                boolean shouldShow = false;
                long timeStampToSecond = timeStamp / 1000;
                if (cmpOperator == null) {
                    if (timeStampToSecond == toCmp) {
                        shouldShow = true;
                    }
                } else if (cmpOperator.equals("<")) {
                    if (timeStampToSecond <= toCmp) {
                        shouldShow = true;
                    }
                } else {
                    if (timeStampToSecond >= toCmp) {
                        shouldShow = true;
                    }
                }
                if (shouldShow) {
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode(schemaName, charset));
                    row.add(StringUtil.encode(tableName, charset));
                    row.add(StringUtil.encode(FormatUtil.formatDate(timeStamp), charset));
                    row.add(StringUtil.encode(createQuery, charset));
                    String tableID = schemaName + "." + tableName;
                    if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableID)) {
                        row.add(LongUtil.toBytes(0L));
                    } else {
                        row.add(LongUtil.toBytes(1L));
                    }
                    if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.contains(tableID)) {
                        row.add(LongUtil.toBytes(0L));
                    } else {
                        row.add(LongUtil.toBytes(1L));
                    }
                    list.add(row);
                }
            }

        }
        return list;
    }

    private static List<RowDataPacket> getTableRows(String schemaName, String tableName, String charset) {
        List<RowDataPacket> list = new ArrayList<>();

        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        SchemaMeta schemaMeta = schemaMetaMap.get(schemaName);
        if (schemaMeta != null) {
            TableMeta tableMeta = schemaMeta.getTableMetas().get(tableName);
            if (tableMeta != null) {
                RowDataPacket row = genNormalRowData(schemaName, tableName, tableMeta.getCreateSql(), tableMeta.getVersion(), charset);
                list.add(row);
            } else {
                if (schemaConfigMap.get(schemaName).isNoSharding() && schemaMeta.getViewMeta(tableName) != null) {
                    RowDataPacket row = genNormalRowData(schemaName, tableName, schemaMeta.getViewMeta(tableName).getCreateSql(), schemaMeta.getViewMeta(tableName).getTimestamp(), charset);
                    list.add(row);
                } else {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, tableName);
                    list.add(row);
                }
            }
        } else {
            SchemaConfig configSchema = schemaConfigMap.get(schemaName);
            if (configSchema != null) {
                if (configSchema.getTables().containsKey(tableName)) {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, tableName);
                    list.add(row);
                }
            }
        }
        return list;
    }

    private static List<RowDataPacket> getSchemaRows(String schemaName, String charset) {
        List<RowDataPacket> list = new ArrayList<>();

        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        SchemaMeta schemaMeta = schemaMetaMap.get(schemaName);
        if (schemaMeta != null) {
            Set<String> hasMetaTables = new HashSet<>();
            for (Map.Entry<String, TableMeta> tableMetaEntry : schemaMeta.getTableMetas().entrySet()) {
                String tableName = tableMetaEntry.getKey();
                hasMetaTables.add(tableName);
                TableMeta tableMeta = tableMetaEntry.getValue();
                RowDataPacket row = genNormalRowData(schemaName, tableName, tableMeta.getCreateSql(), tableMeta.getVersion(), charset);
                list.add(row);
            }
            if (schemaConfigMap.get(schemaName).isNoSharding()) {
                for (Map.Entry<String, ViewMeta> viewMetaEntry : schemaMeta.getViewMetas().entrySet()) {
                    String viewName = viewMetaEntry.getKey();
                    hasMetaTables.add(viewName);
                    ViewMeta viewMeta = viewMetaEntry.getValue();
                    RowDataPacket row = genNormalRowData(schemaName, viewName, viewMeta.getCreateSql(), viewMeta.getTimestamp(), charset);
                    list.add(row);
                }
            }
            for (String configTables : schemaConfigMap.get(schemaName).getTables().keySet()) {
                if (!hasMetaTables.contains(configTables)) {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, configTables);
                    list.add(row);
                }
            }
        } else {
            SchemaConfig configSchema = schemaConfigMap.get(schemaName);
            if (configSchema != null) {
                for (String configTables : configSchema.getTables().keySet()) {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, configTables);
                    list.add(row);
                }
            }
        }
        return list;
    }

    private static RowDataPacket genNormalRowData(String schemaName, String tableName, String createSql, long timestamp, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schemaName, charset));
        row.add(StringUtil.encode(tableName, charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(timestamp), charset));
        row.add(StringUtil.encode(createSql, charset));
        String tableID = schemaName + "." + tableName;
        if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableID)) {
            row.add(LongUtil.toBytes(0L));
        } else {
            row.add(LongUtil.toBytes(1L));
        }
        if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.contains(tableID)) {
            row.add(LongUtil.toBytes(0L));
        } else {
            row.add(LongUtil.toBytes(1L));
        }
        return row;
    }

    private static List<RowDataPacket> getConsistentRows(String filterKey, String filterValue, String charset) {
        List<RowDataPacket> list = new ArrayList<>();
        Set<String> checkFilter;
        boolean checkValue = filterValue.equals("1");
        if (filterKey.equals("consistent_in_sharding_nodes")) {
            checkFilter = ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS;
        } else {
            checkFilter = ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY;
        }

        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        Set<String> hasMetaSchemas = new HashSet<>();
        for (Map.Entry<String, SchemaMeta> schemaMetaEntry : schemaMetaMap.entrySet()) {
            String schemaName = schemaMetaEntry.getKey();
            hasMetaSchemas.add(schemaName);
            SchemaMeta schemaMeta = schemaMetaEntry.getValue();
            Set<String> hasMetaTables = new HashSet<>();
            for (Map.Entry<String, TableMeta> tableMetaEntry : schemaMeta.getTableMetas().entrySet()) {
                String tableName = tableMetaEntry.getKey();
                hasMetaTables.add(tableName);
                String tableID = schemaName + "." + tableName;
                if (checkFilter.contains(tableID) ^ checkValue) {
                    TableMeta tableMeta = tableMetaEntry.getValue();
                    RowDataPacket row = genNormalRowData(schemaName, tableName, tableMeta.getCreateSql(), tableMeta.getVersion(), charset);
                    list.add(row);
                }
            }
            if (!checkValue) {  //checkValue = 0  no metadata will show
                for (String configTables : schemaConfigMap.get(schemaName).getTables().keySet()) {
                    if (!hasMetaTables.contains(configTables)) {
                        //null metadata
                        RowDataPacket row = genNullRowData(charset, schemaName, configTables);
                        list.add(row);
                    }
                }
            }
        }
        if (!checkValue) { //checkValue = 0  no metadata will show
            if (hasMetaSchemas.size() < schemaConfigMap.size()) {
                for (Map.Entry<String, SchemaConfig> schemaConfigEntry : schemaConfigMap.entrySet()) {
                    String configSchema = schemaConfigEntry.getKey();
                    if (!hasMetaSchemas.contains(configSchema)) {
                        for (String configTables : schemaConfigMap.get(configSchema).getTables().keySet()) {
                            //null metadata
                            RowDataPacket row = genNullRowData(charset, configSchema, configTables);
                            list.add(row);
                        }
                    }
                }
            }
        }
        return list;
    }

    private static List<RowDataPacket> getAllRows(String charset) {
        List<RowDataPacket> list = new ArrayList<>();

        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        Set<String> hasMetaSchemas = new HashSet<>();
        SchemaConfig schemaConfig = null;
        for (Map.Entry<String, SchemaMeta> schemaMetaEntry : schemaMetaMap.entrySet()) {
            String schemaName = schemaMetaEntry.getKey();
            hasMetaSchemas.add(schemaName);
            SchemaMeta schemaMeta = schemaMetaEntry.getValue();
            Set<String> hasMetaTables = new HashSet<>();
            for (Map.Entry<String, TableMeta> tableMetaEntry : schemaMeta.getTableMetas().entrySet()) {
                String tableName = tableMetaEntry.getKey();
                hasMetaTables.add(tableName);
                TableMeta tableMeta = tableMetaEntry.getValue();
                RowDataPacket row = genNormalRowData(schemaName, tableName, tableMeta.getCreateSql(), tableMeta.getVersion(), charset);
                list.add(row);
            }
            schemaConfig = schemaConfigMap.get(schemaName);
            if (schemaConfig.isNoSharding()) {
                for (Map.Entry<String, ViewMeta> tableMetaEntry : schemaMeta.getViewMetas().entrySet()) {
                    String viewName = tableMetaEntry.getKey();
                    ViewMeta viewMeta = tableMetaEntry.getValue();
                    RowDataPacket row = genNormalRowData(schemaName, viewName, viewMeta.getCreateSql(), viewMeta.getTimestamp(), charset);
                    list.add(row);
                }
            }
            for (String configTables : schemaConfig.getTables().keySet()) {
                if (!hasMetaTables.contains(configTables)) {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, configTables);
                    list.add(row);
                }
            }
        }
        if (hasMetaSchemas.size() < schemaConfigMap.size()) {
            for (Map.Entry<String, SchemaConfig> schemaConfigEntry : schemaConfigMap.entrySet()) {
                String configSchema = schemaConfigEntry.getKey();
                if (!hasMetaSchemas.contains(configSchema)) {
                    for (String configTables : schemaConfigMap.get(configSchema).getTables().keySet()) {
                        //null metadata
                        RowDataPacket row = genNullRowData(charset, configSchema, configTables);
                        list.add(row);
                    }
                }
            }
        }
        return list;
    }

    private static List<RowDataPacket> getAllNullRows(String charset) {
        List<RowDataPacket> list = new ArrayList<>();

        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        Set<String> hasMetaSchemas = new HashSet<>();
        for (Map.Entry<String, SchemaMeta> schemaMetaEntry : schemaMetaMap.entrySet()) {
            String schemaName = schemaMetaEntry.getKey();
            hasMetaSchemas.add(schemaName);
            SchemaMeta schemaMeta = schemaMetaEntry.getValue();
            Set<String> hasMetaTables = schemaMeta.getTableMetas().keySet();
            for (String configTables : schemaConfigMap.get(schemaName).getTables().keySet()) {
                if (!hasMetaTables.contains(configTables)) {
                    //null metadata
                    RowDataPacket row = genNullRowData(charset, schemaName, configTables);
                    list.add(row);
                }
            }
        }
        if (hasMetaSchemas.size() < schemaConfigMap.size()) {
            for (Map.Entry<String, SchemaConfig> schemaConfigEntry : schemaConfigMap.entrySet()) {
                String configSchema = schemaConfigEntry.getKey();
                if (!hasMetaSchemas.contains(configSchema)) {
                    for (String configTables : schemaConfigMap.get(configSchema).getTables().keySet()) {
                        //null metadata
                        RowDataPacket row = genNullRowData(charset, configSchema, configTables);
                        list.add(row);
                    }
                }
            }
        }
        return list;
    }

    private static RowDataPacket genNullRowData(String charset, String schemaName, String configTables) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schemaName, charset));
        row.add(StringUtil.encode(configTables, charset));
        row.add(StringUtil.encode("null", charset));
        row.add(StringUtil.encode("null", charset));
        row.add(LongUtil.toBytes(0L));
        row.add(LongUtil.toBytes(0L));
        return row;
    }
}
