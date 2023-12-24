/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.JsonFactory;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.cluster.zkprocess.console.ParseParamEnum;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.function.Function;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Schema;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
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
import com.actiontech.dble.route.util.PropertiesUtil;
import com.actiontech.dble.services.manager.response.ReloadConfig;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_CLUSTER;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class ConfigClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(ConfigClusterLogic.class);

    ConfigClusterLogic() {
        super(ClusterOperation.CONFIG);
    }

    public void reloadConfigEvent(ConfStatus value, String params) throws Exception {
        try {
            ClusterDelayProvider.delayBeforeSlaveReload();

            LOGGER.info("reload config from " + ClusterMetaUtil.getConfStatusOperatorPath() + " " + value);
            final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.writeLock().lock();
            try {
                if (!ReloadManager.startReload(TRIGGER_TYPE_CLUSTER, ConfStatus.Status.RELOAD_ALL)) {
                    LOGGER.info("fail to reload config because current dble is in reloading");
                    clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(),
                            FeedBackType.ofError("Reload status error ,other client or cluster may in reload"));
                    return;
                }
                try {
                    ReloadConfig.ReloadResult result = ReloadConfig.reloadByConfig(Integer.parseInt(params), false);
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
            clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), FeedBackType.SUCCESS);
            LOGGER.info("reload config: sent config status success to cluster center end");
        } catch (Exception e) {
            String errorInfo = e.getMessage() == null ? e.toString() : e.getMessage();
            LOGGER.info("reload config: sent config status failed to cluster center start");
            clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), FeedBackType.ofError(errorInfo));
            LOGGER.info("reload config: sent config status failed to cluster center end");
        }
    }

    private boolean checkLocalResult(boolean result) throws Exception {
        if (!result) {
            LOGGER.info("reload config: sent config status success to cluster center start");
            ClusterDelayProvider.delayAfterSlaveReload();
            clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), FeedBackType.ofError("interrupt by command.should reload config again"));
        }
        return result;
    }


    /**
     * sequence
     * properties -> cluster
     *
     * @throws Exception
     */
    public void syncSequencePropsToCluster() throws Exception {

        if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
            RawJson json = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_FILE_NAME);
            clusterHelper.setKV(ClusterMetaUtil.getSequencesCommonPath(), json);
            LOGGER.info("Sequence To cluster: " + ConfigFileName.SEQUENCE_FILE_NAME + " ,success");
        } else if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
            RawJson json = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_DB_FILE_NAME);
            clusterHelper.setKV(ClusterMetaUtil.getSequencesCommonPath(), json);
            LOGGER.info("Sequence To cluster: " + ConfigFileName.SEQUENCE_DB_FILE_NAME + " ,success");
        }
    }

    /**
     * sequence
     * json -> cluster
     *
     * @throws Exception
     */
    public void syncSequenceJsonToCluster() throws Exception {
        RawJson sequenceConfig = DbleServer.getInstance().getConfig().getSequenceConfig();
        if (null == sequenceConfig) {
            LOGGER.info("sequence config is null");
            return;
        }

        clusterHelper.setKV(ClusterMetaUtil.getSequencesCommonPath(), sequenceConfig);
        LOGGER.info("Sequence To cluster: " + sequenceConfig + " ,success");
    }


    public void syncSequenceToLocal(RawJson sequenceConfig, boolean isWriteToLocal) throws Exception {

        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }
        boolean loadByCluster = ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL || ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT;
        if (loadByCluster && sequenceConfig != null) {
            SequenceConverter sequenceConverter = new SequenceConverter();
            Properties props = sequenceConverter.jsonToProperties(sequenceConfig);
            PropertiesUtil.storeProps(props, sequenceConverter.getFileName());
            LOGGER.info("Sequence To Local: " + sequenceConverter.getFileName() + " ,success");
        } else {
            LOGGER.warn("Sequence To Local: get empty value");
        }
    }

    public void syncSequenceJson(String path, RawJson value) throws Exception {
        LOGGER.info("start sync sequence json config:key[{}],value[{}]", path, value);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        DbleTempConfig.getInstance().setSequenceConfig(value);

        LOGGER.info("end sync sequence json config:key[{}],value[{}]", path, value);
    }


    public void syncDbXmlToLocal(XmlProcessBase xmlParseBase, RawJson dbConfig, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.DB_XML + " start:" + dbConfig);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        DbGroups dbs = this.parseDbGroupsJsonToBean(JsonFactory.getJson(), dbConfig, true);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.DB_XML;

        LOGGER.info("cluster to local xml write Path :" + path);

        xmlParseBase.baseParseAndWriteToXml(dbs, path, "db");

        LOGGER.info("cluster to local xml write :" + path + " is success");
    }

    public void syncDbJson(String path, RawJson value) throws Exception {
        LOGGER.info("start sync db json config:key[{}],value[{}]", path, value);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        DbleTempConfig.getInstance().setDbConfig(value);

        LOGGER.info("end sync db json config:key[{}],value[{}]", path, value);
    }

    /**
     * db
     * xml -> cluster
     *
     * @throws Exception
     */
    public void syncDbXmlToCluster() throws Exception {

        LOGGER.info(ConfigFileName.DB_XML + " local to cluster start");
        RawJson json = DBConverter.dbXmlToJson();
        clusterHelper.setKV(ClusterMetaUtil.getDbConfPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * db
     * json -> cluster
     *
     * @throws Exception
     */
    public void syncDbJsonToCluster() throws Exception {
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        if (null == dbConfig) {
            LOGGER.info("db config is null");
            return;
        }
        clusterHelper.setKV(ClusterMetaUtil.getDbConfPath(), dbConfig);
        LOGGER.info("db json config to cluster write is success");
    }

    /**
     * sharding
     * xml -> cluster
     *
     * @throws Exception
     */
    public void syncShardingXmlToCluster() throws Exception {
        LOGGER.info(ConfigFileName.SHARDING_XML + " local to cluster start");
        ShardingConverter shardingConverter = new ShardingConverter();
        RawJson json = shardingConverter.shardingXmlToJson();
        clusterHelper.setKV(ClusterMetaUtil.getConfShardingPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * sharding
     * json -> cluster
     *
     * @throws Exception
     */
    public void syncShardingJsonToCluster() throws Exception {
        RawJson shardingConfig = DbleServer.getInstance().getConfig().getShardingConfig();
        if (null == shardingConfig) {
            LOGGER.info("sharding config is null");
            return;
        }
        clusterHelper.setKV(ClusterMetaUtil.getConfShardingPath(), shardingConfig);
        LOGGER.info("sharding json config to cluster write is success");
    }

    public void syncShardingXmlToLocal(RawJson shardingConfig, XmlProcessBase xmlParseBase, Gson gson, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.SHARDING_XML + " start:" + shardingConfig);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        //the config Value in ucore is an all in one json config of the sharding.xml
        Shardings sharding = this.parseShardingJsonToBean(gson, shardingConfig);
        this.handlerMapFileAddFunction(sharding.getFunction(), false);
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.SHARDING_XML;

        LOGGER.info("cluster to local writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(sharding, path, "sharding");

        LOGGER.info("cluster to local write :" + path + " is success");
    }


    public void syncShardingJson(String path, RawJson value) throws Exception {
        LOGGER.info("start sync sharding json config:key[{}],value[{}]", path, value);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock)) {
            return;
        }

        Shardings sharding = this.parseShardingJsonToBean(JsonFactory.getJson(), value);
        this.handlerMapFileAddFunction(sharding.getFunction(), true);
        DbleTempConfig.getInstance().setShardingConfig(value);

        LOGGER.info("end sync sharding json config:key[{}],value[{}]", path, value);
    }


    /**
     * user
     * xml -> cluster
     *
     * @throws Exception
     */
    public void syncUserXmlToCluster() throws Exception {
        LOGGER.info(ConfigFileName.USER_XML + " local to cluster start");
        RawJson json = new UserConverter().userXmlToJson();
        clusterHelper.setKV(ClusterMetaUtil.getUserConfPath(), json);
        LOGGER.info("xml local to cluster write is success");
    }

    /**
     * user
     * json -> cluster
     *
     * @throws Exception
     */
    public void syncUseJsonToCluster() throws Exception {
        RawJson userConfig = DbleServer.getInstance().getConfig().getUserConfig();
        if (null == userConfig) {
            LOGGER.info("user config is null");
            return;
        }
        clusterHelper.setKV(ClusterMetaUtil.getUserConfPath(), userConfig);
        LOGGER.info("user json config to cluster write is success");
    }

    public void syncUserXmlToLocal(RawJson userConfig, XmlProcessBase xmlParseBase, Gson gson, boolean isWriteToLocal) throws Exception {
        LOGGER.info("cluster to local " + ConfigFileName.USER_XML + " start:" + userConfig);
        String lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName).orElse(null);
        if (lock != null && SystemConfig.getInstance().getInstanceName().equals(lock) && !isWriteToLocal) {
            return;
        }

        //the config Value is an all in one json config of the user.xml
        Users users = this.parseUserJsonToBean(gson, userConfig);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.USER_XML;

        LOGGER.info("cluster to local writePath :" + path);

        xmlParseBase.baseParseAndWriteToXml(users, path, "user");

        LOGGER.info("cluster to local write :" + path + " is success");
    }

    public void syncUserJson(String key, RawJson value) throws Exception {
        LOGGER.info("start sync user json config:key[{}],value[{}]", key, value);
        Optional<String> lock = clusterHelper.getPathValue(ClusterMetaUtil.getConfChangeLockPath()).map(ClusterValue::getInstanceName);
        if (lock.isPresent() && SystemConfig.getInstance().getInstanceName().equals(lock.get())) {
            return;
        }

        DbleTempConfig.getInstance().setUserConfig(value);

        LOGGER.info("end sync user json config:key[{}],value[{}]", key, value);
    }


    public Shardings parseShardingJsonToBean(Gson gson, RawJson jsonContent) {
        //from string to json obj
        JsonObject jsonObject = jsonContent.getJsonObject();

        //from json obj to bean bean
        Shardings shardingBean = new Shardings();
        JsonElement schemaJson = jsonObject.get(ClusterPathUtil.SCHEMA);
        if (schemaJson != null) {
            List<Schema> schemaList = new ArrayList<>();
            JsonArray schemaArray = schemaJson.getAsJsonArray();
            for (JsonElement aSchemaArray : schemaArray) {
                JsonObject schemaObj = aSchemaArray.getAsJsonObject();
                JsonElement tableElement = schemaObj.remove("table");
                Schema schemaBean;
                try {
                    schemaBean = gson.fromJson(schemaObj, Schema.class);
                    if (tableElement != null) {
                        List<Object> tables = new ArrayList<>();
                        JsonArray tableArray = tableElement.getAsJsonArray();
                        for (JsonElement tableObj : tableArray) {
                            Table table = gson.fromJson(tableObj, Table.class);
                            tables.add(table);
                        }
                        schemaBean.setTable(tables);

                    }
                } finally {
                    if (tableElement != null) {
                        //don't modify the rawJson. so must put back the removed element
                        schemaObj.add("table", tableElement);
                    }
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

    public DbGroups parseDbGroupsJsonToBean(Gson gson, RawJson jsonContent, boolean syncHaStatus) {
        DbGroups dbs = new DbGroups();
        JsonObject jsonObject = jsonContent.getJsonObject();
        JsonElement dbGroupsJson = jsonObject.get(ClusterPathUtil.DB_GROUP);
        if (dbGroupsJson != null) {
            List<DBGroup> dbGroupList = gson.fromJson(dbGroupsJson.toString(),
                    new TypeToken<List<DBGroup>>() {
                    }.getType());
            dbs.setDbGroup(dbGroupList);
            if (ClusterConfig.getInstance().isClusterEnable() && syncHaStatus) {
                ClusterLogic.forHA().syncHaStatusFromCluster(gson, dbs, dbGroupList);
            }
        }

        JsonElement version = jsonObject.get(ClusterPathUtil.VERSION);
        if (version != null) {
            dbs.setVersion(gson.fromJson(version.toString(), String.class));
        }
        return dbs;
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

    public Users parseUserJsonToBean(Gson gson, RawJson jsonContent) {
        //from string to json obj
        JsonObject jsonObject = jsonContent.getJsonObject();

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
}
