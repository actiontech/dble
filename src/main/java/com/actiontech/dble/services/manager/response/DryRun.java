/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.*;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
        LOGGER.info("reload config(dry-run): load all xml info start");
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(false);
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
            return;
        }

        //check all the config is legal
        List<ErrorInfo> list = new ArrayList<>();
        try {
            loader.testConnection();
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("just test ,not stop reload, catch exception", e);
            }
        }
        try {
            String msg = ConfigUtil.getAndSyncKeyVariables(loader.getDbGroups(), false);
            if (msg != null) {
                list.add(new ErrorInfo("Backend", "WARNING", msg));
            }
        } catch (Exception e) {
            list.add(new ErrorInfo("Backend", "ERROR", e.getMessage()));
        }

        list.addAll(loader.getErrorInfos());

        ServerConfig serverConfig = new ServerConfig(loader);
        VarsExtractorHandler handler = new VarsExtractorHandler(loader.getDbGroups());
        SystemVariables newSystemVariables = handler.execute();
        if (newSystemVariables == null) {
            if (loader.isFullyConfigured()) {
                list.add(new ErrorInfo("Backend", "ERROR", "Get Vars from backend failed,Maybe all backend MySQL can't connected"));
            } else {
                list.add(new ErrorInfo("Backend", "WARNING", "No dbGroup available"));
            }
        } else {
            try {
                if (newSystemVariables.isLowerCaseTableNames()) {
                    serverConfig.reviseLowerCase();
                } else {
                    serverConfig.loadSequence();
                    serverConfig.selfChecking0();
                }
                //table exists check ,if the vars can not be touch ,the table check has no meaning
                tableExistsCheck(list, serverConfig, newSystemVariables.isLowerCaseTableNames());
            } catch (Exception e) {
                list.add(new ErrorInfo("Xml", "ERROR", e.getMessage()));
            }
        }

        userCheck(list, serverConfig);

        if (ClusterConfig.getInstance().isClusterEnable() && !ClusterConfig.getInstance().useZkMode()) {
            ucoreConnectionTest(list);
        } else {
            list.add(new ErrorInfo("Cluster", "NOTICE", "Dble is in single mod"));
        }

        printResult(service, list);
    }

    private static void ucoreConnectionTest(List<ErrorInfo> list) {
        try {
            String selfPath = ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName());
            ClusterHelper.getPathValue(selfPath);
        } catch (Exception e) {
            list.add(new ErrorInfo("Cluster", "ERROR", "Dble in cluster but all the ucore can't connect"));
        }
    }


    private static void tableExistsCheck(List<ErrorInfo> list, ServerConfig serverConfig, boolean isLowerCase) {
        //get All the exists table from all shardingNode
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("table-exists-check");
        try {
            Map<String, Set<String>> tableMap = showShardingNodeTable(serverConfig, isLowerCase, list);

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
                        list.add(new ErrorInfo("Meta", "WARNING", "Table " + schema.getName() + "." + table.getName() + " don't exists in shardingNode[" + sb.toString() + "]"));
                    }
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static Map<String, Set<String>> showShardingNodeTable(ServerConfig serverConfig, boolean isLowerCase, List<ErrorInfo> list) {
        Map<String, Set<String>> result = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(serverConfig.getShardingNodes().size());
        for (ShardingNode shardingNode : serverConfig.getShardingNodes().values()) {
            DryRunGetNodeTablesHandler showTablesHandler = new DryRunGetNodeTablesHandler(counter, shardingNode, result, isLowerCase, list);
            showTablesHandler.execute();
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
            boolean hasServerUser = false;
            for (UserConfig user : userMap.values()) {
                if (user instanceof ManagerUserConfig) {
                    hasManagerUser = true;
                } else if (user instanceof ShardingUserConfig) {
                    hasServerUser = true;
                    schema.addAll(((ShardingUserConfig) user).getSchemas());
                }
            }
            if (!hasServerUser) {
                list.add(new ErrorInfo("Xml", "WARNING", "There is No Server User"));
            } else if (schema.size() <= serverConfig.getSchemas().size()) {
                for (String schemaName : serverConfig.getSchemas().keySet()) {
                    if (!schema.contains(schemaName)) {
                        list.add(new ErrorInfo("Xml", "WARNING", "Schema:" + schemaName + " has no user"));
                    }
                }
            }
            if (!hasManagerUser) {
                list.add(new ErrorInfo("Xml", "WARNING", "There is No Manager User"));
            }
        }
    }
}
