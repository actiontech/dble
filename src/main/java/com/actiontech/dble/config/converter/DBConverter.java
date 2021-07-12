/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.converter;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.cluster.JsonFactory;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.JsonObjectWriter;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.HeartBeat;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
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
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " +
                            ConfigFileName.DB_XML + " version is " + dbs.getVersion() + ".There may be some incompatible config between two versions, please check it";
                    problemReporter.warn(message);
                } else {
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.DB_XML + " version is " + dbs.getVersion() + ".There must be some incompatible config between two versions, please check it";
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
            DbInstanceConfig[] readDbConfList = new DbInstanceConfig[readHostSize];
            int readCnt = 0;
            for (DBInstance dbInstance : dbInstanceList) {
                DbInstanceConfig dbInstanceConfig;
                try {
                    dbInstanceConfig = createDbInstanceConf(dbGroup, dbInstance, problemReporter);
                } catch (Exception e) {
                    throw new ConfigException("db json to map occurred  parse errors, The detailed results are as follows . " + e, e);
                }
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
                    if (readCnt == readHostSize) {
                        throw new ConfigException("dbGroup[" + dbGroupName + "] has no primary instance!");
                    }
                    readDbConfList[readCnt++] = dbInstanceConfig;
                }
            }
            DbGroupConfig dbGroupConf = new DbGroupConfig(dbGroupName, writeDbConf, readDbConfList, delayThreshold, disableHA);
            dbGroupConf.setRwSplitMode(rwSplitMode);
            dbGroupConf.setHeartbeatSQL(heartbeatSQL);
            int heartbeatTimeout = Optional.ofNullable(heartbeat.getTimeout()).orElse(0);
            dbGroupConf.setHeartbeatTimeout(heartbeatTimeout * 1000);
            int heartbeatErrorRetryCount = Optional.ofNullable(heartbeat.getErrorRetryCount()).orElse(1);
            dbGroupConf.setErrorRetryCount(heartbeatErrorRetryCount);

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
        PhysicalDbInstance[] readSources = new PhysicalDbInstance[conf.getReadInstanceConfigs().length];
        int i = 0;
        for (DbInstanceConfig readNode : conf.getReadInstanceConfigs()) {
            readSources[i++] = createDbInstance(conf, readNode, true);
        }

        return new PhysicalDbGroup(conf.getName(), conf, writeSource, readSources, conf.getRwSplitMode());
    }

    private void beanValidate(DbGroups dbs) {
        if (null == dbs) {
            return;
        }
        List<DBGroup> dbGroupList = dbs.getDbGroup();
        if (dbGroupList == null || dbGroupList.isEmpty()) {
            throw new ConfigException("dbGroup is empty");
        }
        for (DBGroup dbGroup : dbGroupList) {
            if (dbGroup.getDbInstance() == null || dbGroup.getDbInstance().isEmpty()) {
                throw new ConfigException("The content of element type \"dbGroup\" is incomplete, it must match \"(heartbeat,dbInstance+)\"");
            }
            if (dbGroup.getHeartbeat() == null) {
                throw new ConfigException("The content of element type \"dbGroup\" is incomplete, it must match \"(heartbeat,dbInstance+)\"");
            }
        }
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
            propertyList.forEach(property -> props.put(property.getName(), property.getValue()));
            ParameterMapping.mapping(poolConfig, props, problemReporter);
            if (props.size() > 0) {
                String[] propItem = new String[props.size()];
                props.keySet().toArray(propItem);
                throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propItem, ","));
            }
            ParameterMapping.checkMappingResult();
        }

        Integer maxCon = dbInstance.getMaxCon();
        Integer minCon = dbInstance.getMinCon();
        int colonIndex = nodeUrl.indexOf(':');
        String ip = nodeUrl.substring(0, colonIndex).trim();
        int port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
        Boolean primary = Optional.ofNullable(dbInstance.getPrimary()).orElse(false);
        DbInstanceConfig conf = new DbInstanceConfig(name, ip, port, nodeUrl, user, password, disabled, primary, usingDecrypt);
        conf.setMaxCon(maxCon);
        conf.setMinCon(minCon);
        conf.setReadWeight(readWeight);
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

    public Map<String, PhysicalDbGroup> getDbGroupMap() {
        return dbGroupMap;
    }
}
