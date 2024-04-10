/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.values.*;
import com.actiontech.dble.cluster.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.cluster.zkprocess.console.ParseParamEnum;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.function.Function;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Schema;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.shardingnode.ShardingNode;
import com.actiontech.dble.cluster.zkprocess.entity.user.BlackList;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.converter.SequenceConverter;
import com.actiontech.dble.config.converter.ShardingConverter;
import com.actiontech.dble.config.converter.UserConverter;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.util.PropertiesUtil;
import com.actiontech.dble.services.manager.response.ReloadContext;
import com.actiontech.dble.services.manager.response.ReloadConfig;
import com.actiontech.dble.services.manager.response.ShowBinlogStatus;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.JSON_LIST;
import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_CLUSTER;

public final class ClusterLogic {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterLogic.class);

    private static Map<String, String> ddlLockMap = new ConcurrentHashMap<>();

    private ClusterLogic() {
    }

    public static void executeViewEvent(String path, String key, String value) throws Exception {
        String[] childNameInfo = key.split(Repository.SCHEMA_VIEW_SPLIT);
        String schema = childNameInfo[0];
        String viewName = childNameInfo[1];

        String[] operatorValue = value.split(Repository.SCHEMA_VIEW_SPLIT);
        String serverId = operatorValue[0];
        String instanceName = SystemConfig.getInstance().getInstanceName();
        if (instanceName.equals(serverId)) {
            return;
        }
        String optionType = operatorValue[1];
        ClusterDelayProvider.delayWhenReponseViewNotic();
        if (Repository.DELETE.equals(optionType)) {
            LOGGER.info("delete view " + path + ":" + value);
            if (!ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().containsKey(viewName)) {
                return;
            }

            ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().remove(viewName);
            ClusterDelayProvider.delayBeforeReponseView();
            ClusterHelper.createSelfTempNode(path, ClusterPathUtil.SUCCESS);
        } else if (Repository.UPDATE.equals(optionType)) {
            LOGGER.info("update view " + path + ":" + value);
            ClusterDelayProvider.delayBeforeReponseGetView();
            String stmt = ClusterHelper.getPathValue(ClusterPathUtil.getViewPath(schema, viewName));
            if (ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName) != null &&
                    ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName).getCreateSql().equals(stmt)) {
                ClusterDelayProvider.delayBeforeReponseView();
                ClusterHelper.createSelfTempNode(path, ClusterPathUtil.SUCCESS);
                return;
            }
            ViewMeta vm = new ViewMeta(schema, stmt, ProxyMeta.getInstance().getTmManager());
            vm.init();
            vm.addMeta(false);

            Map<String, Map<String, String>> viewCreateSqlMap = ProxyMeta.getInstance().getTmManager().getRepository().getViewCreateSqlMap();
            Map<String, String> schemaMap = viewCreateSqlMap.get(schema);
            schemaMap.put(viewName, stmt);

            ClusterDelayProvider.delayBeforeReponseView();
            ClusterHelper.createSelfTempNode(path, ClusterPathUtil.SUCCESS);
        }
    }

    public static void executeBinlogPauseDeleteEvent(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        if (value.equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }
        // delete node
        DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
    }

    public static void executeBinlogPauseEvent(String value) throws Exception {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        if (value.equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }

        //step 2  try to lock all the commit
        DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
        LOGGER.info("start pause for binlog status");
        boolean isPaused = ShowBinlogStatus.waitAllSession();
        if (!isPaused) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getBinlogPauseStatus(), "Error can't wait all session finished ");
            return;
        }
        try {
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getBinlogPauseStatus(), ClusterPathUtil.SUCCESS);
        } catch (Exception e) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            LOGGER.warn("create binlogPause instance failed", e);
        }
    }

    public static void initDDLEvent(String keyName, DDLInfo ddlInfo) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        ddlLockMap.put(fullName, ddlInfo.getFrom());
        LOGGER.info("init of ddl " + schema + " " + table);
        try {
            ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, ddlInfo.getSql());
        } catch (Exception t) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
            throw t;
        }
    }

    public static void processStatusEvent(String keyName, DDLInfo ddlInfo, DDLInfo.DDLStatus status) {
        try {
            switch (status) {
                case INIT:
                    ClusterLogic.initDDLEvent(keyName, ddlInfo);
                    break;
                case SUCCESS:
                    // just release local lock
                    ClusterLogic.ddlUpdateEvent(keyName, ddlInfo);
                    break;
                case FAILED:
                    // just release local lock
                    ClusterLogic.ddlFailedEvent(keyName);
                    break;

                default:
                    break;

            }
        } catch (Exception e) {
            LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
        }

    }

    public static void deleteDDLNodeEvent(DDLInfo ddlInfo, String path) throws Exception {
        LOGGER.info("DDL node " + path + " removed , and DDL info is " + ddlInfo.toString());
    }

    public static void ddlFailedEvent(String keyName) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        LOGGER.info("ddl execute failed notice, table is " + fullName);
        //if the start node executing ddl with error,just release the lock
        ddlLockMap.remove(fullName);
        ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
        ClusterHelper.createSelfTempNode(ClusterPathUtil.getDDLPath(fullName), ClusterPathUtil.SUCCESS);
    }

    public static void ddlUpdateEvent(String keyName, DDLInfo ddlInfo) throws Exception {
        LOGGER.info("ddl execute success notice");
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        // if the start node is done the ddl execute
        ddlLockMap.remove(fullName);
        ClusterDelayProvider.delayBeforeUpdateMeta();
        //to judge the table is be drop
        if (ddlInfo.getType() == DDLInfo.DDLType.DROP_TABLE) {
            ProxyMeta.getInstance().getTmManager().dropTable(schema, table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false);
        } else {
            //else get the latest table meta from db
            ProxyMeta.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), schema, table);
        }

        ClusterDelayProvider.delayBeforeDdlResponse();
        ClusterHelper.createSelfTempNode(ClusterPathUtil.getDDLPath(fullName), ClusterPathUtil.SUCCESS);
    }


    public static void dbGroupChangeEvent(String dbGroupName, String value) {
        int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, "");
        PhysicalDbGroup physicalDBPool = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
        if (null != physicalDBPool) {
            physicalDBPool.changeIntoLatestStatus(value);
        }
        HaConfigManager.getInstance().haFinish(id, null, value);
    }


    public static void dbGroupResponseEvent(String value, String dbGroupName) throws Exception {
        //dbGroup_locks events,we only try to response to the DISABLE,ignore others
        HaInfo info = new HaInfo(value);
        if (info.getLockType() == HaInfo.HaType.DISABLE &&
                !info.getStartId().equals(SystemConfig.getInstance().getInstanceName()) &&
                info.getStatus() == HaInfo.HaStatus.SUCCESS) {
            try {
                //start the log
                int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, HaInfo.HaStage.RESPONSE_NOTIFY.toString());
                //try to get the latest status of the dbGroup
                String latestStatus = ClusterHelper.getPathValue(ClusterPathUtil.getHaStatusPath(info.getDbGroupName()));
                //find out the target dbGroup and change it into latest status
                PhysicalDbGroup dbGroup = DbleServer.getInstance().getConfig().getDbGroups().get(info.getDbGroupName());
                dbGroup.changeIntoLatestStatus(latestStatus);
                //response the event ,only disable event has response
                ClusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dbGroupName), ClusterPathUtil.SUCCESS);
                //ha manager writeOut finish log
                HaConfigManager.getInstance().haFinish(id, null, latestStatus);
            } catch (Exception e) {
                //response the event ,only disable event has response
                ClusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dbGroupName), e.getMessage());
            }
        }
    }

    public static void reloadConfigEvent(String value, String params) throws Exception {
        try {
            ClusterDelayProvider.delayBeforeSlaveReload();
            LOGGER.info("reload_all " + ClusterPathUtil.getConfStatusOperatorPath() + " " + value);
            final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.writeLock().lock();
            try {
                if (!ReloadManager.startReload(TRIGGER_TYPE_CLUSTER, ConfStatus.Status.RELOAD_ALL)) {
                    LOGGER.info("reload config failed because self is in reloading");
                    ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(),
                            "Reload status error ,other client or cluster may in reload");
                    return;
                }
                try {
                    ReloadConfig.ReloadResult result = ReloadConfig.reloadByConfig(Integer.parseInt(params), false, new ReloadContext());
                    if (!checkLocalResult(result.isSuccess())) {
                        return;
                    }
                } catch (Exception e) {
                    LOGGER.warn("reload config for cluster error: ", e);
                    throw e;
                } finally {
                    ReloadManager.reloadFinish();
                }

            } finally {
                lock.writeLock().unlock();
            }
            ClusterDelayProvider.delayAfterSlaveReload();
            LOGGER.info("reload config: sent config status success to cluster center start");
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), ClusterPathUtil.SUCCESS);
            LOGGER.info("reload config: sent config status success to cluster center end");
        } catch (Exception e) {
            String errorInfo = e.getMessage() == null ? e.toString() : e.getMessage();
            LOGGER.info("reload config: sent config status failed to cluster center start");
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), errorInfo);
            LOGGER.info("reload config: sent config status failed to cluster center end");
        }
    }

    private static boolean checkLocalResult(boolean result) throws Exception {
        if (!result) {
            LOGGER.info("reload config: sent config status success to cluster center start");
            ClusterDelayProvider.delayAfterSlaveReload();
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), "interrupt by command.should reload config again");
        }
        return result;
    }

    public static Thread pauseShardingNodeEvent(String value, final Lock lock) throws Exception {
        final PauseInfo pauseInfo = new PauseInfo(value);
        if (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            return null;
        }
        final String shardingNodes = pauseInfo.getShardingNodes();
        Thread waitThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Strat pause shardingNode " + shardingNodes);
                    Set<String> shardingNodeSet = new HashSet<>(Arrays.asList(shardingNodes.split(",")));
                    PauseShardingNodeManager.getInstance().startPausing(pauseInfo.getConnectionTimeOut(), shardingNodeSet, shardingNodes, pauseInfo.getQueueLimit());

                    while (!Thread.currentThread().isInterrupted()) {
                        lock.lock();
                        try {
                            boolean nextTurn = false;
                            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                                    if (!entry.getValue().isManager()) {
                                        ShardingService shardingService = (ShardingService) entry.getValue().getService();
                                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : shardingService.getSession2().getTargetMap().entrySet()) {
                                            if (shardingNodeSet.contains(conEntry.getKey().getName())) {
                                                nextTurn = true;
                                                break;
                                            }
                                        }
                                        if (nextTurn) {
                                            break;
                                        }
                                    }
                                }
                                if (nextTurn) {
                                    break;
                                }
                            }
                            if (!nextTurn) {
                                LOGGER.info("create self pause node.");
                                ClusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResultNodePath(), ClusterPathUtil.SUCCESS);
                                break;
                            }
                        } finally {
                            lock.unlock();
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
                    }
                    LOGGER.info("pause for slave done. interruptedFlag:" + Thread.currentThread().isInterrupted());

                } catch (Exception e) {
                    LOGGER.warn(" the ucore pause error " + e.getMessage());
                }

            }
        });
        waitThread.start();
        return waitThread;

    }

    public static void resumeShardingNodeEvent(String value, final Lock lock, Thread waitThread) throws Exception {
        final PauseInfo pauseInfo = new PauseInfo(value);
        if (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            return;
        }
        lock.lock();
        try {
            if (waitThread != null && waitThread.isAlive()) {
                waitThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
        LOGGER.info("resume shardingNode for get notice");
        PauseShardingNodeManager.getInstance().resume();
        ClusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResumePath(), SystemConfig.getInstance().getInstanceName());
    }


    /**
     * sequence
     * properties -> cluster
     *
     * @throws Exception
     */
    public static void syncSequencePropsToCluster() throws Exception {
        if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
            String json = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_FILE_NAME);
            ClusterHelper.setKV(ClusterPathUtil.getSequencesCommonPath(), json);
            LOGGER.info("Sequence To cluster: " + ConfigFileName.SEQUENCE_FILE_NAME + " ,success");
        } else if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
            String json = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_DB_FILE_NAME);
            ClusterHelper.setKV(ClusterPathUtil.getSequencesCommonPath(), json);
            LOGGER.info("Sequence To cluster: " + ConfigFileName.SEQUENCE_DB_FILE_NAME + " ,success");
        }
    }

    /**
     * sequence
     * json -> cluster
     *
     * @throws Exception
     */
    public static void syncSequenceJsonToCluster() throws Exception {
        String sequenceConfig = DbleServer.getInstance().getConfig().getSequenceConfig();
        if (null == sequenceConfig) {
            LOGGER.info("sequence config is null");
            return;
        }
        ClusterHelper.setKV(ClusterPathUtil.getSequencesCommonPath(), sequenceConfig);
        LOGGER.info("Sequence To cluster: " + sequenceConfig + " ,success");
    }


    public static void syncSequenceToLocal(String sequenceConfig, boolean isWriteToLocal) throws Exception {
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }
        boolean loadByCluster = ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL || ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT;
        if (loadByCluster && !StringUtil.isEmpty(sequenceConfig)) {
            SequenceConverter sequenceConverter = new SequenceConverter();
            Properties props = sequenceConverter.jsonToProperties(sequenceConfig);
            PropertiesUtil.storeProps(props, sequenceConverter.getFileName());
            LOGGER.info("Sequence To Local: " + sequenceConverter.getFileName() + " ,success");
        } else {
            LOGGER.warn("Sequence To Local: get empty value");
        }
    }

    public static void syncSequenceJson(KvBean configValue) throws Exception {
        LOGGER.info("start sync sequence json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        DbleTempConfig.getInstance().setSequenceConfig(configValue.getValue());

        LOGGER.info("end sync sequence json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
    }

    private static void changeDbGroupByStatus(DBGroup dbGroup, List<DbInstanceStatus> statusList) {
        Map<String, DbInstanceStatus> statusMap = new HashMap<>(statusList.size());
        for (DbInstanceStatus status : statusList) {
            statusMap.put(status.getName(), status);
        }
        for (DBInstance instance : dbGroup.getDbInstance()) {
            DbInstanceStatus status = statusMap.get(instance.getName());
            if (null != status) {
                instance.setPrimary(status.isPrimary());
                instance.setDisabled(status.isDisable() ? "true" : "false");
            }
        }
    }

    public static Map<String, DBGroup> changeFromListToMap(List<DBGroup> dbGroupList) {
        Map<String, DBGroup> dbGroupMap = new HashMap<>(dbGroupList.size());
        for (DBGroup dbGroup : dbGroupList) {
            dbGroupMap.put(dbGroup.getName(), dbGroup);
        }
        return dbGroupMap;
    }

    public static Shardings parseShardingJsonToBean(Gson gson, String jsonContent) {
        //from string to json obj
        JsonObject jsonObject = new JsonParser().parse(jsonContent).getAsJsonObject();

        //from json obj to bean bean
        Shardings shardingBean = new Shardings();
        JsonElement schemaJson = jsonObject.get(ClusterPathUtil.SCHEMA);
        if (schemaJson != null) {
            List<Schema> schemaList = new ArrayList<>();
            JsonArray schemaArray = schemaJson.getAsJsonArray();
            for (JsonElement aSchemaArray : schemaArray) {
                JsonObject schemaObj = aSchemaArray.getAsJsonObject();
                JsonElement tableElement = schemaObj.remove("table");
                Schema schemaBean = gson.fromJson(schemaObj, Schema.class);
                if (tableElement != null) {
                    List<Object> tables = new ArrayList<>();
                    JsonArray tableArray = tableElement.getAsJsonArray();
                    for (JsonElement tableObj : tableArray) {
                        Table table = gson.fromJson(tableObj, Table.class);
                        tables.add(table);
                    }
                    schemaBean.setTable(tables);
                }
                schemaList.add(schemaBean);
            }
            shardingBean.setSchema(schemaList);
        }
        JsonElement shardingNodeJson = jsonObject.get(ClusterPathUtil.SHARDING_NODE);
        if (shardingNodeJson != null) {
            List<ShardingNode> shardingNodeList = gson.fromJson(shardingNodeJson.toString(), new TypeToken<List<ShardingNode>>() {
            }.getType());
            shardingBean.setShardingNode(shardingNodeList);
        }

        JsonElement functionJson = jsonObject.get(ClusterPathUtil.FUNCTION);
        if (functionJson != null) {
            List<Function> functions = gson.fromJson(functionJson.toString(), new TypeToken<List<Function>>() {
            }.getType());
            shardingBean.setFunction(functions);
        }
        JsonElement version = jsonObject.get(ClusterPathUtil.VERSION);
        if (version != null) {
            shardingBean.setVersion(gson.fromJson(version.toString(), String.class));
        }
        return shardingBean;
    }

    public static DbGroups parseDbGroupsJsonToBean(Gson gson, String jsonContent, boolean syncHaStatus) {
        DbGroups dbs = new DbGroups();
        JsonObject jsonObject = new JsonParser().parse(jsonContent).getAsJsonObject();
        JsonElement dbGroupsJson = jsonObject.get(ClusterPathUtil.DB_GROUP);
        if (dbGroupsJson != null) {
            List<DBGroup> dbGroupList = gson.fromJson(dbGroupsJson.toString(),
                    new TypeToken<List<DBGroup>>() {
                    }.getType());
            dbs.setDbGroup(dbGroupList);
            if (ClusterConfig.getInstance().isClusterEnable() && syncHaStatus) {
                syncHaStatusFromCluster(gson, dbs, dbGroupList);
            }
        }

        JsonElement version = jsonObject.get(ClusterPathUtil.VERSION);
        if (version != null) {
            dbs.setVersion(gson.fromJson(version.toString(), String.class));
        }
        return dbs;
    }

    private static void changStatusByJson(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList, String dbGroupName, DBGroup dbGroup, String data) {
        if (dbGroup != null) {
            JsonObject jsonStatusObject = new JsonParser().parse(data).getAsJsonObject();
            JsonElement instanceJson = jsonStatusObject.get(JSON_LIST);
            if (instanceJson != null) {
                List<DbInstanceStatus> list = gson.fromJson(instanceJson.toString(),
                        new TypeToken<List<DbInstanceStatus>>() {
                        }.getType());
                dbs.setDbGroup(dbGroupList);
                changeDbGroupByStatus(dbGroup, list);
            }
        } else {
            LOGGER.warn("dbGroup " + dbGroupName + " is not found");
        }
    }

    private static void syncHaStatusFromCluster(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList) {
        try {
            List<KvBean> statusKVList = getKVBeanOfChildPath(ClusterPathUtil.getHaStatusPath());
            if (statusKVList != null && statusKVList.size() > 0) {
                Map<String, DBGroup> dbGroupMap = changeFromListToMap(dbGroupList);
                for (KvBean kv : statusKVList) {
                    String[] path = kv.getKey().split(ClusterPathUtil.SEPARATOR);
                    String dbGroupName = path[path.length - 1];
                    DBGroup dbGroup = dbGroupMap.get(dbGroupName);
                    changStatusByJson(gson, dbs, dbGroupList, dbGroupName, dbGroup, kv.getValue());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("syncHaStatusFromCluster error :", e);
        }
    }

    public static Users parseUserJsonToBean(Gson gson, String jsonContent) {
        //from string to json obj
        JsonObject jsonObject = new JsonParser().parse(jsonContent).getAsJsonObject();

        //from json obj to bean bean
        Users usersBean = new Users();
        JsonElement userElement = jsonObject.get(ClusterPathUtil.USER);
        if (userElement != null) {
            List<Object> users = new ArrayList<>();
            JsonArray userArray = userElement.getAsJsonArray();
            for (JsonElement userObj : userArray) {
                User user = gson.fromJson(userObj, User.class);
                users.add(user);
            }
            usersBean.setUser(users);
        }
        JsonElement blacklistJson = jsonObject.get(ClusterPathUtil.BLACKLIST);
        if (blacklistJson != null) {
            List<BlackList> blacklistList = gson.fromJson(blacklistJson.toString(), new TypeToken<List<BlackList>>() {
            }.getType());
            usersBean.setBlacklist(blacklistList);
        }
        JsonElement version = jsonObject.get(ClusterPathUtil.VERSION);
        if (version != null) {
            usersBean.setVersion(gson.fromJson(version.toString(), String.class));
        }
        return usersBean;
    }

    public static int getPathHeight(String path) {
        if (path.endsWith(ClusterPathUtil.SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        return path.split(ClusterPathUtil.SEPARATOR).length;
    }

    public static List<KvBean> getKVBeanOfChildPath(String path) throws Exception {
        List<KvBean> allList = ClusterHelper.getKVPath(path);
        int parentHeight = getPathHeight(path);
        Iterator<KvBean> iter = allList.iterator();
        while (iter.hasNext()) {
            KvBean bean = iter.next();
            String[] key = bean.getKey().split(ClusterPathUtil.SEPARATOR);
            if (key.length != parentHeight + 1) {
                iter.remove();
            }
        }
        return allList;
    }

    public static boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        Map<String, String> currentMap = ClusterHelper.getOnlineMap();
        checkOnline(expectedMap, currentMap);
        List<KvBean> responseList;
        try {
            responseList = getKVBeanOfChildPath(path);
        } catch (Exception e) {
            LOGGER.warn("checkResponseForOneTime error :", e);
            errorMsg.append(e.getMessage());
            return true;
        }
        if (expectedMap.size() == 0) {
            if (errorMsg.length() != 0) {
                errorMsg.append("All online key dropped, other instance config may out of sync, try again manually");
            }
            return true;
        }

        boolean flag = true;
        for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
            boolean found = false;
            for (KvBean kvBean : responseList) {
                String responseNode = lastItemOfArray(kvBean.getKey().split(ClusterPathUtil.SEPARATOR));
                if (lastItemOfArray(entry.getKey().split(ClusterPathUtil.SEPARATOR)).equals(responseNode)) {
                    if (!StringUtil.isEmpty(checkString)) {
                        if (!checkString.equals(kvBean.getValue())) {
                            if (errorMsg != null) {
                                errorMsg.append(responseNode).append(":").append(kvBean.getValue()).append(";");
                            }
                        }
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                flag = false;
            }
        }

        return flag;
    }

    private static <T> T lastItemOfArray(T[] array) {
        return array[array.length - 1];
    }

    private static void checkOnline(Map<String, String> expectedMap, Map<String, String> currentMap) {
        expectedMap.entrySet().removeIf(entry -> !currentMap.containsKey(entry.getKey()) ||
                (currentMap.containsKey(entry.getKey()) && !currentMap.get(entry.getKey()).equals(entry.getValue())));

        for (Map.Entry<String, String> entry : currentMap.entrySet()) {
            if (!expectedMap.containsKey(entry.getKey())) {
                LOGGER.warn("NODE " + entry.getKey() + " IS NOT EXPECTED TO BE ONLINE,PLEASE CHECK IT ");
            }
        }
    }

    public static String waitingForAllTheNode(String path, String checkString) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("wait-for-others-cluster");
        try {
            Map<String, String> expectedMap = ClusterHelper.getOnlineMap();
            StringBuffer errorMsg = new StringBuffer();
            for (; ; ) {
                errorMsg.setLength(0);
                if (checkResponseForOneTime(checkString, path, expectedMap, errorMsg)) {
                    break;
                }
            }
            return errorMsg.length() <= 0 ? null : errorMsg.toString();
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public static void syncDbXmlToLocal(XmlProcessBase xmlParseBase, String dbConfig, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.DB_XML + " start:" + dbConfig);
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        DbGroups dbs = ClusterLogic.parseDbGroupsJsonToBean(new Gson(), dbConfig, true);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.DB_XML;

        LOGGER.info("cluster to local xml write Path :" + path);

        xmlParseBase.baseParseAndWriteToXml(dbs, path, "db");

        LOGGER.info("cluster to local xml write :" + path + " is success");
    }

    public static void syncDbJson(KvBean configValue) throws Exception {
        LOGGER.info("start sync db json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        DbleTempConfig.getInstance().setDbConfig(configValue.getValue());

        LOGGER.info("end sync db json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
    }

    /**
     * db
     * xml -> cluster
     *
     * @throws Exception
     */
    public static void syncDbXmlToCluster() throws Exception {
        LOGGER.info(ConfigFileName.DB_XML + " local to cluster start");
        String json = DBConverter.dbXmlToJson();
        ClusterHelper.setKV(ClusterPathUtil.getDbConfPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * db
     * json -> cluster
     *
     * @throws Exception
     */
    public static void syncDbJsonToCluster() throws Exception {
        String dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        if (null == dbConfig) {
            LOGGER.info("db config is null");
            return;
        }
        ClusterHelper.setKV(ClusterPathUtil.getDbConfPath(), dbConfig);
        LOGGER.info("db json config to cluster write is success");
    }

    /**
     * sharding
     * xml -> cluster
     *
     * @throws Exception
     */
    public static void syncShardingXmlToCluster() throws Exception {
        LOGGER.info(ConfigFileName.SHARDING_XML + " local to cluster start");
        ShardingConverter shardingConverter = new ShardingConverter();
        String json = shardingConverter.shardingXmlToJson();
        ClusterHelper.setKV(ClusterPathUtil.getConfShardingPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * sharding
     * json -> cluster
     *
     * @throws Exception
     */
    public static void syncShardingJsonToCluster() throws Exception {
        String shardingConfig = DbleServer.getInstance().getConfig().getShardingConfig();
        if (null == shardingConfig) {
            LOGGER.info("sharding config is null");
            return;
        }
        ClusterHelper.setKV(ClusterPathUtil.getConfShardingPath(), shardingConfig);
        LOGGER.info("sharding json config to cluster write is success");
    }

    public static void syncShardingXmlToLocal(String shardingConfig, XmlProcessBase xmlParseBase, Gson gson, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.SHARDING_XML + " start:" + shardingConfig);
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        //the config Value in ucore is an all in one json config of the sharding.xml
        Shardings sharding = ClusterLogic.parseShardingJsonToBean(gson, shardingConfig);
        handlerMapFileAddFunction(sharding.getFunction(), false);
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.SHARDING_XML;

        LOGGER.info("cluster to local writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(sharding, path, "sharding");

        LOGGER.info("cluster to local write :" + path + " is success");
    }


    public static void syncShardingJson(KvBean configValue) throws Exception {
        LOGGER.info("start sync sharding json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        Shardings sharding = ClusterLogic.parseShardingJsonToBean(gsonBuilder.create(), configValue.getValue());
        handlerMapFileAddFunction(sharding.getFunction(), true);
        DbleTempConfig.getInstance().setShardingConfig(configValue.getValue());

        LOGGER.info("end sync sharding json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
    }

    private static void handlerMapFileAddFunction(List<Function> functionList, boolean write) {
        if (functionList == null) {
            return;
        }
        List<Property> tempData = new ArrayList<>();
        List<Property> writeData = new ArrayList<>();
        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                for (Property property : proList) {
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        tempData.add(property);
                    }
                }

                if (!tempData.isEmpty()) {
                    for (Property property : tempData) {
                        for (Property prozkdownload : proList) {
                            if (property.getValue().equals(prozkdownload.getName())) {
                                writeData.add(prozkdownload);
                            }
                        }
                    }
                }
                if (write) {
                    writeMapFileAddFunction(writeData);
                }
                proList.removeAll(writeData);
                tempData.clear();
                writeData.clear();
            }
        }
    }

    private static void writeMapFileAddFunction(List<Property> writeData) {
        if (!writeData.isEmpty()) {
            for (Property writeMsg : writeData) {
                try {
                    ConfFileRWUtils.writeFile(writeMsg.getName(), writeMsg.getValue());
                } catch (IOException e) {
                    LOGGER.warn("write File IOException", e);
                }
            }
        }
    }


    /**
     * user
     * xml -> cluster
     *
     * @throws Exception
     */
    public static void syncUserXmlToCluster() throws Exception {
        LOGGER.info(ConfigFileName.USER_XML + " local to cluster start");
        String json = new UserConverter().userXmlToJson();
        ClusterHelper.setKV(ClusterPathUtil.getUserConfPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * user
     * json -> cluster
     *
     * @throws Exception
     */
    public static void syncUseJsonToCluster() throws Exception {
        String userConfig = DbleServer.getInstance().getConfig().getUserConfig();
        if (null == userConfig) {
            LOGGER.info("user config is null");
            return;
        }
        ClusterHelper.setKV(ClusterPathUtil.getUserConfPath(), userConfig);
        LOGGER.info("user json config to cluster write is success");
    }

    public static void syncUserXmlToLocal(String userConfig, XmlProcessBase xmlParseBase, Gson gson, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.USER_XML + " start:" + userConfig);
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        //the config Value is an all in one json config of the user.xml
        Users users = ClusterLogic.parseUserJsonToBean(gson, userConfig);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.USER_XML;

        LOGGER.info("cluster to local writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(users, path, "user");

        LOGGER.info("cluster to local write :" + path + " is success");
    }

    public static void syncUserJson(KvBean configValue) throws Exception {
        LOGGER.info("start sync user json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
        String lock = ClusterHelper.getPathValue(ClusterPathUtil.getConfChangeLockPath());
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        DbleTempConfig.getInstance().setUserConfig(configValue.getValue());

        LOGGER.info("end sync user json config:key[{}],value[{}]", configValue.getKey(), configValue.getValue());
    }

    public static void syncDbGroupStatusToCluster() throws Exception {
        LOGGER.info("syncDbGroupStatusToCluster start");
        HaConfigManager.getInstance().init(false);
        Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(entry.getKey()), entry.getValue());
        }
        LOGGER.info("syncDbGroupStatusToCluster success");
    }

    public static void syncDbGroupStatusToCluster(ReloadConfig.ReloadResult reloadResult) throws Exception {
        LOGGER.info("syncDbGroupStatusToCluster start");
        HaConfigManager.getInstance().init(true);
        Map<String, String> dbGroupStatusMap = HaConfigManager.getInstance().getSourceJsonList();

        Map<String, PhysicalDbGroup> recycleHostMap = reloadResult.getRecycleHostMap();
        if (recycleHostMap != null) {
            for (Map.Entry<String, PhysicalDbGroup> groupEntry : recycleHostMap.entrySet()) {
                String dbGroupName = groupEntry.getKey();
                LOGGER.debug("delete dbGroup_status:{}", dbGroupName);
                ClusterHelper.cleanKV(ClusterPathUtil.getHaStatusPath(dbGroupName));
            }
        }
        Map<String, PhysicalDbGroup> addOrChangeHostMap = reloadResult.getAddOrChangeHostMap();
        if (addOrChangeHostMap != null) {
            for (Map.Entry<String, PhysicalDbGroup> groupEntry : addOrChangeHostMap.entrySet()) {
                String dbGroupStatusJson = dbGroupStatusMap.get(groupEntry.getKey());
                LOGGER.debug("add dbGroup_status:{}---{}", groupEntry.getKey(), dbGroupStatusJson);
                ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(groupEntry.getKey()), dbGroupStatusJson);
            }
        }
        LOGGER.info("syncDbGroupStatusToCluster success");
    }

    public static void checkBinlogStatusRelease(String crashNode) {
        try {
            //check the latest bing_log status
            String fromNode = ClusterHelper.getPathValue(ClusterPathUtil.getBinlogPauseStatus());
            if (StringUtil.isEmpty(fromNode)) {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            } else if (crashNode.equals(fromNode)) {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                String myselfPath = ClusterPathUtil.getBinlogPauseStatus() + ClusterPathUtil.SEPARATOR + SystemConfig.getInstance().getInstanceName();
                KvBean myself = ClusterHelper.getKV(myselfPath);
                boolean needDelete = true;
                long beginTime = TimeUtil.currentTimeMillis();
                long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
                while (myself == null || StringUtil.isEmpty(myself.getValue())) {
                    //wait 2* timeout to release itself
                    if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                        LOGGER.warn("checkExists of " + myselfPath + " time out");
                        needDelete = false;
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    myself = ClusterHelper.getKV(myselfPath);
                }
                if (needDelete) {
                    ClusterHelper.cleanKV(myselfPath);
                }
                LOGGER.warn(" service instance[" + crashNode + "] has crashed. " +
                        "Please manually make sure node [" + ClusterPathUtil.getBinlogPauseStatus() + "] status in cluster " +
                        "after every instance received this message");
                ClusterHelper.cleanPath(ClusterPathUtil.getBinlogPauseStatus());
                ClusterHelper.cleanKV(ClusterPathUtil.getBinlogPauseLockPath());
            }
        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error: ", e);
        }
    }

    public static void checkDDLAndRelease(String crashNode) {
        //deal with the status when the ddl is init notified
        //and than the ddl server is shutdown
        Set<String> tableToDel = new HashSet<>();
        for (Map.Entry<String, String> en : ddlLockMap.entrySet()) {
            if (crashNode.equals(en.getValue())) {
                String fullName = en.getKey();
                String[] tableInfo = fullName.split("\\.");
                final String schema = StringUtil.removeBackQuote(tableInfo[0]);
                final String table = StringUtil.removeBackQuote(tableInfo[1]);
                ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
                tableToDel.add(fullName);
                ddlLockMap.remove(fullName);
            }
        }
        if (tableToDel.size() > 0) {
            try {
                List<KvBean> kvs = ClusterLogic.getKVBeanOfChildPath(ClusterPathUtil.getDDLPath());
                for (KvBean kv : kvs) {
                    String path = kv.getKey();
                    String[] paths = path.split(ClusterPathUtil.SEPARATOR);
                    String keyName = paths[paths.length - 1];
                    String[] tableInfo = keyName.split("\\.");
                    final String schema = StringUtil.removeBackQuote(tableInfo[0]);
                    final String table = StringUtil.removeBackQuote(tableInfo[1]);
                    String fullName = schema + "." + table;
                    if (tableToDel.contains(fullName)) {
                        ClusterHelper.cleanPath(path);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(" service instance[" + crashNode + "] has crashed. " +
                        "Please manually check ddl status on cluster and delete ddl path[" + tableToDel + "] from cluster ");
            }
        }
    }
}
