/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config.converter;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.nio.MySQLInstance;
import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.JsonObjectWriter;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.DbGroups;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.Property;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups.HeartBeat;
import com.oceanbase.obsharding_d.cluster.zkprocess.parse.XmlProcessBase;
import com.oceanbase.obsharding_d.config.ConfigFileName;
import com.oceanbase.obsharding_d.config.ProblemReporter;
import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.config.model.db.DbGroupConfig;
import com.oceanbase.obsharding_d.config.model.db.DbInstanceConfig;
import com.oceanbase.obsharding_d.config.model.db.PoolConfig;
import com.oceanbase.obsharding_d.config.model.db.type.DataBaseType;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.config.util.ParameterMapping;
import com.oceanbase.obsharding_d.util.DecryptUtil;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBConverter.class);
    public static final String DB_NAME_FORMAT = "a-zA-Z_0-9\\-\\.";
    public static final Pattern PATTERN_DB = Pattern.compile("([" + DB_NAME_FORMAT + "]+)", Pattern.CASE_INSENSITIVE);

    private final Map<String, PhysicalDbGroup> dbGroupMap = Maps.newLinkedHashMap();

    public static RawJson dbXmlToJson() throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(DbGroups.class);
        xmlProcess.initJaxbClass();
        return parseDbGroupXmlFileToJson(xmlProcess);
    }

    public static RawJson dbXmlToJson(String xmlPath) throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(DbGroups.class);
        xmlProcess.initJaxbClass();
        return parseDbGroupXmlFileToJson(xmlProcess, xmlPath, ConfigFileName.DB_XSD);
    }

    public DbGroups dbJsonToBean(RawJson dbJson, boolean syncHaStatus) {
        return ClusterLogic.forConfig().parseDbGroupsJsonToBean(JsonFactory.getJson(), dbJson, syncHaStatus);
    }

    public static RawJson dbBeanToJson(DbGroups dbGroups) {
        Gson gson = JsonFactory.getJson();
        // bean to json obj
        JsonObjectWriter jsonObj = new JsonObjectWriter();
        jsonObj.addProperty(ClusterPathUtil.VERSION, dbGroups.getVersion());

        jsonObj.add(ClusterPathUtil.DB_GROUP, gson.toJsonTree(dbGroups.getDbGroup()));
        return RawJson.of(jsonObj);
    }

    public void dbJsonToMap(RawJson dbJson, ProblemReporter problemReporter, boolean syncHaStatus) {
        DbGroups dbs = dbJsonToBean(dbJson, syncHaStatus);
        if (dbs.getVersion() != null && !Versions.CONFIG_VERSION.equals(dbs.getVersion())) {
            if (problemReporter != null) {
                if (Versions.checkVersion(dbs.getVersion())) {
                    String message = "The OBsharding-D-config-version is " + Versions.CONFIG_VERSION + ",but the " +
                            ConfigFileName.DB_XML + " version is " + dbs.getVersion() + ".There may be some incompatible config between two versions, please check it";
                    problemReporter.warn(message);
                } else {
                    String message = "The OBsharding-D-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.DB_XML + " version is " + dbs.getVersion() + ".There must be some incompatible config between two versions, please check it";
                    problemReporter.warn(message);
                }
            }
        }
        for (DBGroup dbGroup : dbs.getDbGroup()) {
            String dbGroupName = dbGroup.getName();
            Matcher nameMatcher = PATTERN_DB.matcher(dbGroupName);
            if (!nameMatcher.matches()) {
                throw new ConfigException("dbGroup name " + dbGroupName + " show be use " + DB_NAME_FORMAT + "!");
            }
            if (this.dbGroupMap.containsKey(dbGroupName)) {
                throw new ConfigException("dbGroup name " + dbGroupName + " duplicated!");
            }
            List<DBInstance> dbInstanceList = dbGroup.getDbInstance();
            int delayThreshold = Optional.ofNullable(dbGroup.getDelayThreshold()).orElse(-1);
            String disableHAStr = dbGroup.getDisableHA();
            boolean disableHA = Boolean.parseBoolean(Optional.ofNullable(disableHAStr).orElse("false"));
            int rwSplitMode = dbGroup.getRwSplitMode();
            HeartBeat heartbeat = dbGroup.getHeartbeat();
            String heartbeatSQL = heartbeat.getValue();

            Set<String> instanceNames = new HashSet<>();
            Set<String> instanceUrls = new HashSet<>();
            int readHostSize = dbInstanceList.size() - 1;
            DbInstanceConfig writeDbConf = null;
            List<DbInstanceConfig> readInstanceConfigList = Lists.newArrayList();
            DataBaseType dataBaseType = null;
            for (DBInstance dbInstance : dbInstanceList) {
                DbInstanceConfig dbInstanceConfig;
                try {
                    dbInstanceConfig = createDbInstanceConf(dbGroup, dbInstance, problemReporter);
                } catch (Exception e) {
                    throw new ConfigException("db json to map occurred  parse errors, The detailed results are as follows . " + e, e);
                }
                dataBaseType = Optional.ofNullable(dataBaseType).orElse(dbInstanceConfig.getDataBaseType());
                String instanceName = dbInstanceConfig.getInstanceName();
                String instanceUrl = dbInstanceConfig.getUrl();
                Optional.of(instanceName).filter(currentName -> !instanceNames.contains(currentName)).orElseThrow(() ->
                        new ConfigException("dbGroup[" + dbGroupName + "]'s child host name [" + instanceName + "]  duplicated!"));
                instanceNames.add(instanceName);
                Optional.of(instanceUrl).filter(currentUrl -> !instanceUrls.contains(currentUrl)).orElseThrow(() ->
                        new ConfigException("dbGroup[" + dbGroupName + "]'s child url [" + instanceUrl + "]  duplicated!"));
                instanceUrls.add(instanceUrl);
                if (dbInstanceConfig.isPrimary()) {
                    if (writeDbConf == null) {
                        writeDbConf = dbInstanceConfig;
                    } else {
                        throw new ConfigException("dbGroup[" + dbGroupName + "] has multi primary instance!");
                    }
                } else {
                    if (readInstanceConfigList.size() == readHostSize) {
                        throw new ConfigException("dbGroup[" + dbGroupName + "] has no primary instance!");
                    }
                    readInstanceConfigList.add(dbInstanceConfig);
                }
                if (!dataBaseType.equals(dbInstanceConfig.getDataBaseType())) {
                    throw new ConfigException("dbGroup[" + dbGroupName + "]'s child database type must be consistent");
                }
            }
            int delayPeriodMillis = Optional.ofNullable(dbGroup.getDelayPeriodMillis()).orElse(-1);
            String delayDatabase = dbGroup.getDelayDatabase();
            DbGroupConfig dbGroupConf = new DbGroupConfig(dbGroupName, writeDbConf, readInstanceConfigList, delayThreshold, disableHA);
            dbGroupConf.setRwSplitMode(rwSplitMode);
            dbGroupConf.setHeartbeatSQL(heartbeatSQL);
            dbGroupConf.setDelayDatabase(delayDatabase);
            dbGroupConf.setDelayPeriodMillis(delayPeriodMillis);
            int heartbeatTimeout = Optional.ofNullable(heartbeat.getTimeout()).orElse(0);
            dbGroupConf.setHeartbeatTimeout(heartbeatTimeout * 1000);
            int heartbeatErrorRetryCount = Optional.ofNullable(heartbeat.getErrorRetryCount()).orElse(1);
            dbGroupConf.setErrorRetryCount(heartbeatErrorRetryCount);
            int heartbeatKeepAlive = Optional.ofNullable(heartbeat.getKeepAlive()).orElse(60);
            dbGroupConf.setKeepAlive(heartbeatKeepAlive);

            PhysicalDbGroup physicalDbGroup = getPhysicalDBPoolSingleWH(dbGroupConf);
            this.dbGroupMap.put(dbGroupConf.getName(), physicalDbGroup);
        }
    }

    private static PhysicalDbInstance createDbInstance(DbGroupConfig conf, DbInstanceConfig node, boolean isRead) {
        return new MySQLInstance(node, conf, isRead);
    }

    private static PhysicalDbGroup getPhysicalDBPoolSingleWH(DbGroupConfig conf) {
        //create PhysicalDbInstance for writeDirectly host
        PhysicalDbInstance writeSource = createDbInstance(conf, conf.getWriteInstanceConfig(), false);
        PhysicalDbInstance[] readSources = new PhysicalDbInstance[conf.getReadInstanceConfigs().size()];
        int i = 0;
        for (DbInstanceConfig readNode : conf.getReadInstanceConfigs()) {
            readSources[i++] = createDbInstance(conf, readNode, true);
        }

        return new PhysicalDbGroup(conf.getName(), conf, writeSource, readSources, conf.getRwSplitMode());
    }

    private static RawJson parseDbGroupXmlFileToJson(XmlProcessBase xmlParseBase) throws Exception {
        return parseDbGroupXmlFileToJson(xmlParseBase, ConfigFileName.DB_XML, ConfigFileName.DB_XSD);
    }

    private static RawJson parseDbGroupXmlFileToJson(XmlProcessBase xmlParseBase, String xmlPath, String xsdPath) throws Exception {
        // xml file to bean
        DbGroups groupsBean;
        try {
            groupsBean = (DbGroups) xmlParseBase.baseParseXmlToBean(xmlPath, xsdPath);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to DbGroups is :" + groupsBean);
        }
        return dbBeanToJson(groupsBean);
    }

    private DbInstanceConfig createDbInstanceConf(DBGroup dbGroup, DBInstance dbInstance, ProblemReporter problemReporter) throws InvocationTargetException, IllegalAccessException {
        String name = dbInstance.getName();
        String nodeUrl = dbInstance.getUrl();
        String user = dbInstance.getUser();
        String password = dbInstance.getPassword();
        String usingDecryptStr = dbInstance.getUsingDecrypt();

        Matcher nameMatcher = PATTERN_DB.matcher(name);
        if (!nameMatcher.matches()) {
            throw new ConfigException("dbInstance name " + name + " show be use " + DB_NAME_FORMAT + "!");
        }
        if (StringUtil.isEmpty(name) || StringUtil.isEmpty(nodeUrl) || StringUtil.isEmpty(user)) {
            throw new ConfigException(
                    "dbGroup " + dbGroup.getName() +
                            " define error,some attributes of this element is empty: " +
                            name);
        }
        boolean usingDecrypt = Boolean.parseBoolean(Optional.ofNullable(usingDecryptStr).orElse("false"));
        password = DecryptUtil.dbHostDecrypt(usingDecrypt, name, user, password);
        String disabledStr = dbInstance.getDisabled();
        boolean disabled = Boolean.parseBoolean(Optional.ofNullable(disabledStr).orElse("false"));
        String readWeightStr = dbInstance.getReadWeight();
        List<Property> propertyList = dbInstance.getProperty();
        int readWeight = Integer.parseInt(Optional.ofNullable(readWeightStr).orElse("0"));
        if (readWeight < 0) {
            throw new ConfigException("readWeight attribute in dbInstance[" + name + "] can't be less than 0!");
        }
        // init properties of connection pool
        PoolConfig poolConfig = new PoolConfig();
        if (!propertyList.isEmpty()) {
            Properties props = new Properties();
            List<String> errorMsgList = Lists.newArrayList();
            for (Property property : propertyList) {
                checkProperty(errorMsgList, property);
                props.setProperty(property.getName(), property.getValue());
            }
            ParameterMapping.mapping(poolConfig, props, problemReporter);
            if (props.size() > 0) {
                String[] propItem = new String[props.size()];
                props.keySet().toArray(propItem);
                throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propItem, ","));
            }
            ParameterMapping.checkMappingResult();
            if (errorMsgList.size() > 0) {
                throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(errorMsgList, ","));
            }
        }

        DataBaseType dataBaseType = getDatabaseType(dbInstance.getDatabaseType());
        Integer maxCon = dbInstance.getMaxCon();
        Integer minCon = dbInstance.getMinCon();
        int colonIndex = nodeUrl.indexOf(':');
        String ip = nodeUrl.substring(0, colonIndex).trim();
        int port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
        Boolean primary = Optional.ofNullable(dbInstance.getPrimary()).orElse(false);
        DbInstanceConfig conf = new DbInstanceConfig(name, ip, port, nodeUrl, user, password, disabled, primary, usingDecrypt, dataBaseType);
        conf.setMaxCon(maxCon);
        conf.setMinCon(minCon);
        conf.setReadWeight(readWeight);

        String dbDistrict = dbInstance.getDbDistrict();
        checkChineseAndRules(dbDistrict, "dbDistrict");
        String dbDataCenter = dbInstance.getDbDataCenter();
        checkChineseAndRules(dbDataCenter, "dbDataCenter");
        conf.setDbDistrict(dbDistrict);
        conf.setDbDataCenter(dbDataCenter);
        // id
        String id = dbInstance.getId();
        if (StringUtil.isEmpty(id)) {
            conf.setId(name);
        } else {
            conf.setId(id);
        }
        conf.setPoolConfig(poolConfig);
        return conf;
    }

    private DataBaseType getDatabaseType(String dbType) {
        dbType = Optional.ofNullable(dbType).orElse("mysql");
        DataBaseType dataBaseType;
        if (!StringUtil.equals(dbType, dbType.toLowerCase())) {
            throw new ConfigException("databaseType [" + dbType + "]  use lowercase");
        }
        try {
            dataBaseType = DataBaseType.valueOf(dbType.toUpperCase());
        } catch (Exception e) {
            throw new ConfigException("databaseType [" + dbType + "] not support");
        }
        return dataBaseType;
    }

    private void checkChineseAndRules(String val, String name) {
        if (Objects.nonNull(val)) {
            if (StringUtil.isBlank(val)) {
                throw new ConfigException("property [ " + name + " ] " + val + " is illegal, the value not be empty");
            }
            int length = 11;
            if (val.length() > length) {
                throw new ConfigException("property [ " + name + " ] " + val + " is illegal, the value contains a maximum of  " + length + "  characters");
            }
            String chinese = val.replaceAll(PATTERN_DB.toString(), "");
            if (Strings.isNullOrEmpty(chinese)) {
                return;
            }
            if (!StringUtil.isChinese(chinese)) {
                throw new ConfigException("properties of system may not recognized:" + val + "the " + Charset.defaultCharset().name() + " encoding is recommended, dbInstance name " + name + " show be use  u4E00-u9FA5a-zA-Z_0-9\\-\\.");
            }
        }

    }

    private void checkProperty(List<String> errorMsgList, Property property) {
        String value = property.getValue();
        if (StringUtil.isBlank(value)) {
            return;
        }

        switch (property.getName()) {
            case "testOnCreate":
            case "testOnBorrow":
            case "testOnReturn":
            case "testWhileIdle":
                if (!StringUtil.equalsIgnoreCase(value, Boolean.FALSE.toString()) && !StringUtil.equalsIgnoreCase(value, Boolean.TRUE.toString())) {
                    errorMsgList.add("property [ " + property.getName() + " ] '" + value + "' data type should be boolean");
                }
                break;
            case "connectionTimeout":
            case "connectionHeartbeatTimeout":
            case "timeBetweenEvictionRunsMillis":
            case "idleTimeout":
            case "heartbeatPeriodMillis":
            case "evictorShutdownTimeoutMillis":
            case "flowHighLevel":
            case "flowLowLevel":
                if (!StringUtil.isBlank(value)) {
                    if (!LongUtil.isLong(value)) {
                        errorMsgList.add("property [ " + property.getName() + " ] '" + value + "' data type should be long");
                    } else if (LongUtil.parseLong(value) <= 0) {
                        errorMsgList.add("property [ " + property.getName() + " ] '" + value + "' should be an integer greater than 0!");
                    }
                }
                break;
            default:
                break;
        }
    }

    public Map<String, PhysicalDbGroup> getDbGroupMap() {
        return dbGroupMap;
    }
}
