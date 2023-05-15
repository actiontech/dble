/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.*;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.converter.SequenceConverter;
import com.actiontech.dble.config.converter.ShardingConverter;
import com.actiontech.dble.config.converter.UserConverter;
import com.actiontech.dble.config.helper.ShowDatabaseHandler;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.meta.table.DryRunGetNodeTablesHandler;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/8/6.
 */
public final class DryRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(DryRun.class);
    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DETAIL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }


    private DryRun() {
    }

    public static void execute(ManagerService service) {
        try {
            LOGGER.info("[dry-run] start load all xml info");
            ConfigInitializer loader;
            try {
                //sync json
                RawJson userConfig = new UserConverter().userXmlToJson();
                RawJson dbConfig = DBConverter.dbXmlToJson();
                RawJson shardingConfig = new ShardingConverter().shardingXmlToJson();
                RawJson sequenceConfig = null;
                if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
                    sequenceConfig = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_FILE_NAME);
                } else if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
                    sequenceConfig = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_DB_FILE_NAME);
                }
                loader = new ConfigInitializer(userConfig, dbConfig, shardingConfig, sequenceConfig);
            } catch (Exception e) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage() == null ? e.toString() : e.getMessage());
                return;
            }

            //check all the config is legal
            List<ErrorInfo> list = new CopyOnWriteArrayList<>();
            try {
                LOGGER.info("[dry-run] test connection from backend start");
                loader.testConnection();
            } catch (Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[dry-run] test connection, catch exception", e);
                }
            }

            try {
                LOGGER.info("[dry-run] check dble and mysql version start");
                //check version
                ConfigUtil.checkDbleAndMysqlVersion(loader.getDbGroups());
            } catch (Exception e) {
                LOGGER.debug("[dry-run] check dble and mysql version, catch exception:", e);
                list.add(new ErrorInfo("Backend", "ERROR", "check dble and mysql version exception: " + e.getMessage()));
            }

            try {
                LOGGER.info("[dry-run] check and get system variables from backend start");
                List<String> syncKeyVariables = ConfigUtil.getAndSyncKeyVariables(loader.getDbGroups(), false);
                for (String syncKeyVariable : syncKeyVariables) {
                    list.add(new ErrorInfo("Backend", "WARNING", syncKeyVariable));
                }
            } catch (Exception e) {
                LOGGER.debug("[dry-run] check and get system variables, catch exception:", e);
                list.add(new ErrorInfo("Backend", "ERROR", "get system variables exception: " + e.getMessage()));
            }

            list.addAll(loader.getErrorInfos());

            ServerConfig serverConfig = new ServerConfig(loader);

            LOGGER.info("[dry-run] get variables from backend start");
            VarsExtractorHandler handler = new VarsExtractorHandler(loader.getDbGroups());
            SystemVariables newSystemVariables = handler.execute();
            if (newSystemVariables == null) {
                if (loader.isFullyConfigured()) {
                    list.add(new ErrorInfo("Backend", "ERROR", "Get Vars from backend failed, Maybe all backend MySQL can't connected"));
                } else {
                    list.add(new ErrorInfo("Backend", "WARNING", "No dbGroup available"));
                }
            } else {
                try {
                    if (newSystemVariables.isLowerCaseTableNames()) {
                        serverConfig.reviseLowerCase();
                    }
                    serverConfig.selfChecking0();
                    serverConfig.tryLoadSequence(loader.getSequenceConfig(), LOGGER);

                    Map<String, Set<String>> schemaMap = getExistSchemas(serverConfig);
                    //table exists check ,if the vars can not be touch ,the table check has no meaning
                    tableExistsCheck(list, serverConfig, newSystemVariables.isLowerCaseTableNames(), schemaMap);
                } catch (Exception e) {
                    LOGGER.debug("[dry-run] get variables exception: ", e);
                    list.add(new ErrorInfo("Xml", "ERROR", "get variables exception: " + e.getMessage()));
                }
            }

            LOGGER.info("[dry-run] check user start");
            userCheck(list, serverConfig);

            if (ClusterConfig.getInstance().isClusterEnable() && !ClusterConfig.getInstance().useZkMode()) {
                ucoreConnectionTest(list);
            }
            if (!ClusterConfig.getInstance().isClusterEnable()) {
                list.add(new ErrorInfo("Cluster", "NOTICE", "Dble is in single mod"));
            }

            LOGGER.info("[dry-run] check delay detection start");
            delayDetection(serverConfig, list);

            printResult(service, list);
        } finally {
            LOGGER.info("[dry-run] end ...");
        }
    }

    private static void delayDetection(ServerConfig serverConfig, List<ErrorInfo> list) {
        Map<String, PhysicalDbGroup> dbGroups = serverConfig.getDbGroups();
        dbGroups.forEach((k, v) -> {
            DataBaseType dataBaseType = v.getDbGroupConfig().instanceDatabaseType();
            if (dataBaseType == DataBaseType.MYSQL && v.isDelayDetectionStart()) {
                String delayDatabase = v.getDbGroupConfig().getDelayDatabase();
                ShowDatabaseHandler mysqlShowDatabaseHandler = new ShowDatabaseHandler(dbGroups, "Database");
                Set<String> databases = mysqlShowDatabaseHandler.execute(v.getWriteDbInstance());
                if (!databases.contains(delayDatabase)) {
                    list.add(new ErrorInfo("Xml", "WARNING", "database " + delayDatabase + " doesn't exists in dbGroup[" + v.getGroupName() + "]"));
                }
            }
        });
    }

    private static void ucoreConnectionTest(List<ErrorInfo> list) {
        try {
            String selfPath = ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName());
            ClusterHelper.isExist(selfPath);
        } catch (Exception e) {
            list.add(new ErrorInfo("Cluster", "ERROR", "Dble in cluster but all the ucore can't connect"));
        }
    }

    private static Map<String, Set<String>> getExistSchemas(ServerConfig serverConfig) {
        Map<String, Set<String>> schemaMap = Maps.newHashMap();
        Map<String, PhysicalDbGroup> dbGroups = serverConfig.getDbGroups();
        Map<String, PhysicalDbGroup> mysqlDbGroups = Maps.newHashMap();
        Map<String, PhysicalDbGroup> clickHouseDbGroups = Maps.newHashMap();
        dbGroups.forEach((k, v) -> {
            DataBaseType dataBaseType = v.getDbGroupConfig().instanceDatabaseType();
            if (dataBaseType == DataBaseType.MYSQL) {
                mysqlDbGroups.put(k, v);
            } else {
                clickHouseDbGroups.put(k, v);
            }
        });

        if (!mysqlDbGroups.isEmpty()) {
            ShowDatabaseHandler mysqlShowDatabaseHandler = new ShowDatabaseHandler(mysqlDbGroups, "Database");
            schemaMap.putAll(getSchemaMap(mysqlShowDatabaseHandler));
        }
        if (!clickHouseDbGroups.isEmpty()) {
            ShowDatabaseHandler clickHouseDatabaseHandler = new ShowDatabaseHandler(clickHouseDbGroups, "name");
            schemaMap.putAll(getSchemaMap(clickHouseDatabaseHandler));
        }
        return schemaMap;
    }

    private static Map<String, Set<String>> getSchemaMap(ShowDatabaseHandler databaseHandler) {
        Map<String, Set<String>> schemaMap = new HashMap<>();
        List<PhysicalDbInstance> physicalDbInstances = databaseHandler.getPhysicalDbInstances();
        physicalDbInstances.forEach(ds -> {
            Set<String> schemaSet = databaseHandler.execute(ds);
            schemaMap.put(ds.getDbGroup().getGroupName(), schemaSet);
        });
        return schemaMap;
    }

    private static void tableExistsCheck(List<ErrorInfo> list, ServerConfig serverConfig, boolean isLowerCase, Map<String, Set<String>> schemaMap) {
        //get All the exists table from all shardingNode
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("table-exists-check");
        try {
            Map<String, Set<String>> tableMap = showShardingNodeTable(serverConfig, isLowerCase, list, schemaMap);

            for (SchemaConfig schema : serverConfig.getSchemas().values()) {
                for (BaseTableConfig table : schema.getTables().values()) {
                    StringBuilder sb = new StringBuilder(100);
                    for (String exDn : table.getShardingNodes()) {
                        if (tableMap.get(exDn) != null && !tableMap.get(exDn).contains(table.getName())) {
                            sb.append(exDn).append(",");
                        }
                    }

                    if (sb.length() > 1) {
                        sb.setLength(sb.length() - 1);
                        list.add(new ErrorInfo("Meta", "WARNING", "Table " + schema.getName() + "." + table.getName() + " doesn't exists in shardingNode(s)[" + sb.toString() + "]"));
                    }
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static Map<String, Set<String>> showShardingNodeTable(ServerConfig serverConfig, boolean isLowerCase, List<ErrorInfo> list, Map<String, Set<String>> schemaMap) {
        Map<String, Set<String>> result = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(serverConfig.getShardingNodes().size());
        for (ShardingNode shardingNode : serverConfig.getShardingNodes().values()) {
            String dbGroupName = shardingNode.getDbGroupName();
            String databaseName = shardingNode.getDatabase();
            if (schemaMap.containsKey(dbGroupName)) {
                Set<String> schemaSet = schemaMap.get(dbGroupName);
                boolean exist;
                if (isLowerCase) {
                    Optional<String> existSchema = schemaSet.stream().filter(schema -> StringUtil.equals(schema.toLowerCase(), databaseName)).findFirst();
                    exist = existSchema.isPresent();
                } else {
                    exist = schemaSet.contains(databaseName);
                }
                if (exist) {
                    DryRunGetNodeTablesHandler showTablesHandler = new DryRunGetNodeTablesHandler(counter, shardingNode, result, isLowerCase, list);
                    showTablesHandler.execute();
                } else {
                    counter.decrementAndGet();
                    list.add(new ErrorInfo("Meta", "WARNING", "Database " + shardingNode.getDatabase() + " doesn't exists in dbGroup[" + shardingNode.getDbGroupName() + "]"));
                }
            } else {
                counter.decrementAndGet();
                list.add(new ErrorInfo("Meta", "WARNING", "Database " + shardingNode.getDatabase() + " doesn't exists in dbGroup[" + shardingNode.getDbGroupName() + "]"));
            }
        }
        while (counter.get() != 0) {
            LockSupport.parkNanos(1000L);
        }
        return result;
    }


    private static void printResult(ManagerService service, List<ErrorInfo> list) {
        ByteBuffer buffer = service.allocate();
        // write header
        buffer = HEADER.write(buffer, service, true);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        buffer = EOF.write(buffer, service, true);
        // write rows
        byte packetId = EOF.getPacketId();

        Collections.sort(list, new Comparator<ErrorInfo>() {
            @Override
            public int compare(ErrorInfo o1, ErrorInfo o2) {
                if (o1.getLevel().equals(o2.getLevel())) {
                    return 0;
                } else if (o1.getLevel().equals("ERROR")) {
                    return -1;
                } else if (o1.getLevel().equals("NOTICE")) {
                    return 1;
                } else if (o2.getLevel().equals("ERROR")) {
                    return 1;
                }
                return -1;
            }
        });

        for (ErrorInfo info : list) {
            RowDataPacket row = getRow(info, service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }


    private static RowDataPacket getRow(ErrorInfo info, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(info.getType(), charset));
        row.add(StringUtil.encode(info.getLevel(), charset));
        row.add(StringUtil.encode(info.getDetail(), charset));
        return row;
    }


    private static void userCheck(List<ErrorInfo> list, ServerConfig serverConfig) {
        Map<UserName, UserConfig> userMap = serverConfig.getUsers();
        if (userMap != null && userMap.size() > 0) {
            Set<String> schema = new HashSet<>();
            boolean hasManagerUser = false;
            boolean hasShardingUser = false;
            boolean hasRWSplitUser = false;
            for (UserConfig user : userMap.values()) {
                if (user instanceof ManagerUserConfig) {
                    hasManagerUser = true;
                } else if (user instanceof ShardingUserConfig) {
                    hasShardingUser = true;
                    schema.addAll(((ShardingUserConfig) user).getSchemas());
                } else {
                    hasRWSplitUser = true;
                }
            }
            if (!hasShardingUser) {
                if (serverConfig.getSchemas().size() > 0) {
                    list.add(new ErrorInfo("Xml", "WARNING", "There is No Sharding User"));
                } else {
                    list.add(new ErrorInfo("Xml", "NOTICE", "There is No Sharding User"));
                }
            } else if (schema.size() <= serverConfig.getSchemas().size()) {
                for (String schemaName : serverConfig.getSchemas().keySet()) {
                    if (!schema.contains(schemaName)) {
                        list.add(new ErrorInfo("Xml", "WARNING", "Schema:" + schemaName + " has no user"));
                    }
                }
            }

            if (!hasRWSplitUser) {
                if (serverConfig.getSchemas().size() == 0) {
                    list.add(new ErrorInfo("Xml", "WARNING", "There is No RWSplit User"));
                } else {
                    list.add(new ErrorInfo("Xml", "NOTICE", "There is No RWSplit User"));
                }
            }

            if (!hasManagerUser) {
                list.add(new ErrorInfo("Xml", "WARNING", "There is No Manager User"));
            }
        }
    }
}
