package com.actiontech.dble.config.converter;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.HeartBeat;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.loader.xml.XMLDbLoader;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class DBConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBConverter.class);

    private final Map<String, PhysicalDbGroup> dbGroupMap = Maps.newHashMap();

    public static String dbXmlToJson() throws JAXBException, XMLStreamException {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(DbGroups.class);
        xmlProcess.initJaxbClass();
        String path = ClusterPathUtil.LOCAL_WRITE_PATH + ConfigFileName.DB_XML;
        return parseDbGroupXmlFileToJson(xmlProcess, path);
    }

    public DbGroups dbJsonToBean(String dbJson) {
        return ClusterLogic.parseDbGroupsJsonToBean(new Gson(), dbJson);
    }

    public static String dbBeanToJson(DbGroups dbGroups) {
        Gson gson = new Gson();
        // bean to json obj
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty(ClusterPathUtil.VERSION, dbGroups.getVersion());

        jsonObj.add(ClusterPathUtil.DB_GROUP, gson.toJsonTree(dbGroups.getDbGroup()));
        //from json obj to string
        return gson.toJson(jsonObj);
    }

    public void dbJsonToMap(String dbJson, ProblemReporter problemReporter) {
        DbGroups dbs = dbJsonToBean(dbJson);
        for (DBGroup dbGroup : dbs.getDbGroup()) {
            String dbGroupName = dbGroup.getName();
            List<DBInstance> dbInstanceList = dbGroup.getDbInstance();
            Integer delayThreshold = dbGroup.getDelayThreshold();
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

            PhysicalDbGroup physicalDbGroup = XMLDbLoader.getPhysicalDBPoolSingleWH(dbGroupConf);
            this.dbGroupMap.put(dbGroupConf.getName(), physicalDbGroup);
        }
    }

    static String parseDbGroupXmlFileToJson(XmlProcessBase xmlParseBase, String path) throws JAXBException, XMLStreamException {
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
        return dbBeanToJson(groupsBean);
    }

    private DbInstanceConfig createDbInstanceConf(DBGroup dbGroup, DBInstance dbInstance, ProblemReporter problemReporter) throws InvocationTargetException, IllegalAccessException {
        String name = dbInstance.getName();
        String nodeUrl = dbInstance.getUrl();
        String user = dbInstance.getUser();
        String password = dbInstance.getPassword();
        String usingDecryptStr = dbInstance.getUsingDecrypt();

        Matcher nameMatcher = XMLDbLoader.PATTERN_DB.matcher(name);
        if (!nameMatcher.matches()) {
            throw new ConfigException("dbInstance name " + name + " show be use " + XMLDbLoader.DB_NAME_FORMAT + "!");
        }
        if (StringUtil.isEmpty(name) || StringUtil.isEmpty(nodeUrl) || StringUtil.isEmpty(user)) {
            throw new ConfigException(
                    "dbGroup " + dbGroup +
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
            Map<String, String> propertyMap = propertyList.stream().collect(Collectors.toMap(Property::getName, Property::getValue));
            ParameterMapping.mapping(poolConfig, propertyMap, problemReporter);
            if (propertyMap.size() > 0) {
                throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propertyMap.keySet(), ","));
            }
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
