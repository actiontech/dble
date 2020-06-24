package com.actiontech.dble.cluster;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
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
import com.actiontech.dble.cluster.zkprocess.entity.sharding.shardingnode.ShardingNode;
import com.actiontech.dble.cluster.zkprocess.entity.user.BlackList;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DbInstanceStatus;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.util.ZKUtils;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.JSON_LIST;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterHelper.class);
    private ClusterHelper() {

    }

    public static void setKV(String path, String value) throws Exception {
        if (ClusterConfig.getInstance().useZkMode()) {
            ZKUtils.getConnection().create().forPath(path, value.getBytes(StandardCharsets.UTF_8));
        } else {
            ClusterGeneralConfig.getInstance().getClusterSender().setKV(path, value);
        }
    }

    public static KvBean getKV(String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKV(path);
    }

    public static void cleanKV(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanKV(path);
    }

    public static List<KvBean> getKVPath(String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKVPath(path);
    }

    public static int getChildrenSize(String path) throws Exception {
        if (ClusterConfig.getInstance().useZkMode()) {
            return ZKUtils.getConnection().getChildren().forPath(path).size();
        } else {
            return ClusterGeneralConfig.getInstance().getClusterSender().getKVPath(path).size();
        }
    }

    public static void cleanPath(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanPath(path);
    }

    public static boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        return ClusterGeneralConfig.getInstance().getClusterSender().checkResponseForOneTime(checkString, path, expectedMap, errorMsg);
    }

    public static String waitingForAllTheNode(String checkString, String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().waitingForAllTheNode(checkString, path);
    }

    public static void alert(ClusterAlertBean alert) {
        ClusterGeneralConfig.getInstance().getClusterSender().alert(alert);
    }

    public static boolean alertResolve(ClusterAlertBean alert) {
        return ClusterGeneralConfig.getInstance().getClusterSender().alertResolve(alert);
    }

    public static SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception {
        return ClusterGeneralConfig.getInstance().getClusterSender().subscribeKvPrefix(request);
    }

    private static void changeDbGroupByStatus(DBGroup dbGroup, List<DbInstanceStatus> statusList) {
        Map<String, DbInstanceStatus> statusMap = new HashMap<>(statusList.size());
        for (DbInstanceStatus status : statusList) {
            statusMap.put(status.getName(), status);
        }
        for (DBInstance instance : dbGroup.getDbInstance()) {
            DbInstanceStatus status = statusMap.get(instance.getName());
            instance.setPrimary(status.isPrimary());
            instance.setDisabled(status.isDisable() ? "true" : "false");
        }
    }

    public static Map<String, DBGroup> changeFromListToMap(List<DBGroup> dbGroupList) {
        Map<String, DBGroup> dbGroupMap = new HashMap<>(dbGroupList.size());
        for (DBGroup dbGroup : dbGroupList) {
            dbGroupMap.put(dbGroup.getName(), dbGroup);
        }
        return dbGroupMap;
    }

    public static void writeMapFileAddFunction(List<Function> functionList) {
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

                if (!writeData.isEmpty()) {
                    for (Property writeMsg : writeData) {
                        try {
                            ConfFileRWUtils.writeFile(writeMsg.getName(), writeMsg.getValue());
                        } catch (IOException e) {
                            LOGGER.warn("write File IOException", e);
                        }
                    }
                }

                proList.removeAll(writeData);

                tempData.clear();
                writeData.clear();
            }
        }

    }

    public static String parseShardingXmlFileToJson(XmlProcessBase xmlParseBase, Gson gson, String path) throws JAXBException, XMLStreamException {
        // xml file to bean
        Shardings shardingBean;
        try {
            shardingBean = (Shardings) xmlParseBase.baseParseXmlToBean(path);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to Shardings is :" + shardingBean);
        }
        // bean to json obj
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty(ClusterPathUtil.VERSION, shardingBean.getVersion());

        JsonArray schemaArray = new JsonArray();
        for (Schema schema : shardingBean.getSchema()) {
            if (schema.getTable() != null) {
                JsonObject schemaJsonObj = gson.toJsonTree(schema).getAsJsonObject();
                schemaJsonObj.remove("table");
                JsonArray tableArray = new JsonArray();
                for (Object table : schema.getTable()) {
                    JsonElement tableElement = gson.toJsonTree(table, Table.class);
                    tableArray.add(tableElement);
                }
                schemaJsonObj.add("table", gson.toJsonTree(tableArray));
                schemaArray.add(gson.toJsonTree(schemaJsonObj));
            } else {
                schemaArray.add(gson.toJsonTree(schema));
            }
        }
        jsonObj.add(ClusterPathUtil.SCHEMA, gson.toJsonTree(schemaArray));
        jsonObj.add(ClusterPathUtil.SHARDING_NODE, gson.toJsonTree(shardingBean.getShardingNode()));
        List<Function> functionList = shardingBean.getFunction();
        readMapFileAddFunction(functionList);
        jsonObj.add(ClusterPathUtil.FUNCTION, gson.toJsonTree(functionList));
        //from json obj to string
        return gson.toJson(jsonObj);
    }

    private static void readMapFileAddFunction(List<Function> functionList) {
        List<Property> tempData = new ArrayList<>();
        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                for (Property property : proList) {
                    // if mapfile,read and save to json
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        Property mapFilePro = new Property();
                        mapFilePro.setName(property.getValue());
                        try {
                            mapFilePro.setValue(ConfFileRWUtils.readFile(property.getValue()));
                            tempData.add(mapFilePro);
                        } catch (IOException e) {
                            LOGGER.warn("readMapFile IOException", e);
                        }
                    }
                }
                proList.addAll(tempData);
                tempData.clear();
            }
        }
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

    public static DbGroups parseDbGroupsJsonToBean(Gson gson, String jsonContent) {
        DbGroups dbs = new DbGroups();
        JsonObject jsonObject = new JsonParser().parse(jsonContent).getAsJsonObject();
        JsonElement dbGroupsJson = jsonObject.get(ClusterPathUtil.DB_GROUP);
        if (dbGroupsJson != null) {
            List<DBGroup> dbGroupList = gson.fromJson(dbGroupsJson.toString(),
                    new TypeToken<List<DBGroup>>() {
                    }.getType());
            dbs.setDbGroup(dbGroupList);
            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                if (ClusterConfig.getInstance().useZkMode()) {
                    syncHaStatusFromZk(gson, dbs, dbGroupList);
                } else {
                    syncHaStatusFromCluster(gson, dbs, dbGroupList);
                }
            }
        }

        JsonElement version = jsonObject.get(ClusterPathUtil.VERSION);
        if (version != null) {
            dbs.setVersion(gson.fromJson(version.toString(), String.class));
        }
        return dbs;
    }
    private static void syncHaStatusFromZk(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList) {
        try {
            List<String> dbGroupStatusList = ZKUtils.getConnection().getChildren().forPath(ClusterPathUtil.getHaStatusPath());
            if (dbGroupList != null && dbGroupList.size() > 0) {
                Map<String, DBGroup> dbGroupMap = ClusterHelper.changeFromListToMap(dbGroupList);
                for (String dbGroupName : dbGroupStatusList) {
                    DBGroup dbGroup = dbGroupMap.get(dbGroupName);
                    String data = new String(ZKUtils.getConnection().getData().forPath(ZKPaths.makePath(ClusterPathUtil.getHaStatusPath(), dbGroupName)), "UTF-8");
                    changStatusByJson(gson, dbs, dbGroupList, dbGroupName, dbGroup, data);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("get error try to write db.xml");
        }
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
                ClusterHelper.changeDbGroupByStatus(dbGroup, list);
            }
        } else {
            LOGGER.warn("dbGroup " + dbGroupName + " is not found");
        }
    }

    private static void syncHaStatusFromCluster(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList) {
        List<KvBean> statusKVList = ClusterHelper.getKVPath(ClusterPathUtil.getHaStatusPath());
        if (statusKVList != null && statusKVList.size() > 0) {
            Map<String, DBGroup> dbGroupMap = ClusterHelper.changeFromListToMap(dbGroupList);
            for (KvBean kv : statusKVList) {
                String[] path = kv.getKey().split("/");
                String dbGroupName = path[path.length - 1];
                DBGroup dbGroup = dbGroupMap.get(dbGroupName);
                changStatusByJson(gson, dbs, dbGroupList, dbGroupName, dbGroup, kv.getValue());
            }
        }
    }


    public static String parseDbGroupXmlFileToJson(XmlProcessBase xmlParseBase, Gson gson, String path) throws JAXBException, XMLStreamException {
        // xml file to bean
        DbGroups groupsBean;
        try {
            groupsBean = (DbGroups) xmlParseBase.baseParseXmlToBean(path);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to DbGroups is :" + groupsBean);
        }
        // bean to json obj
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty(ClusterPathUtil.VERSION, groupsBean.getVersion());

        jsonObj.add(ClusterPathUtil.DB_GROUP, gson.toJsonTree(groupsBean.getDbGroup()));
        //from json obj to string
        return gson.toJson(jsonObj);
    }

    public static String parseUserXmlFileToJson(XmlProcessBase xmlParseBase, Gson gson, String path) throws JAXBException, XMLStreamException {
        // xml file to bean
        Users usersBean;
        try {
            usersBean = (Users) xmlParseBase.baseParseXmlToBean(path);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to Shardings is :" + usersBean);
        }
        // bean to json obj
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty(ClusterPathUtil.VERSION, usersBean.getVersion());

        JsonArray userArray = new JsonArray();
        for (Object user : usersBean.getUser()) {
            JsonElement tableElement = gson.toJsonTree(user, User.class);
            userArray.add(tableElement);
        }
        jsonObj.add(ClusterPathUtil.USER, gson.toJsonTree(userArray));
        jsonObj.add(ClusterPathUtil.BLACKLIST, gson.toJsonTree(usersBean.getBlacklist()));
        //from json obj to string
        return gson.toJson(jsonObj);
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
    public static synchronized void cleanBackupLocked() {
        if (DbleServer.getInstance().getBackupLocked() != null) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
        }
    }
}
