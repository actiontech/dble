/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unchecked")
public class XMLDbLoader {
    private static final String DEFAULT_DTD = "/db.dtd";
    private static final String DEFAULT_XML = "/" + ConfigFileName.DB_XML;
    public static final String DB_NAME_FORMAT = "a-zA-Z_0-9\\-\\.";
    private final Map<String, DbGroupConfig> dbGroupConfigs;
    private ProblemReporter problemReporter;
    private final Map<String, PhysicalDbGroup> dbGroups;
    private static final Pattern PATTERN_DB = Pattern.compile("([" + DB_NAME_FORMAT + "]+)", Pattern.CASE_INSENSITIVE);

    public XMLDbLoader(String dbFile, ProblemReporter problemReporter) {
        this.dbGroupConfigs = new LinkedHashMap<>();
        this.problemReporter = problemReporter;
        load(DEFAULT_DTD, dbFile == null ? DEFAULT_XML : dbFile);
        this.dbGroups = initDbGroups(dbGroupConfigs);
    }

    public Map<String, PhysicalDbGroup> getDbGroups() {
        return dbGroups;
    }


    private void load(String dtdFile, String xmlFile) {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream(dtdFile);
            xml = ResourceUtil.getResourceAsStream(xmlFile);
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            String version = null;
            if (root.getAttributes().getNamedItem("version") != null) {
                version = root.getAttributes().getNamedItem("version").getNodeValue();
            }
            if (version != null && !Versions.CONFIG_VERSION.equals(version)) {
                if (this.problemReporter != null) {
                    if (Versions.checkVersion(version)) {
                        String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " +
                                ConfigFileName.DB_XML + " version is " + version + ".There may be some incompatible config between two versions, please check it";
                        this.problemReporter.warn(message);
                    } else {
                        String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.DB_XML + " version is " + version + ".There must be some incompatible config between two versions, please check it";
                        this.problemReporter.warn(message);
                    }
                }
            }
            loadDbGroups(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        } finally {

            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                    //ignore error
                }
            }

            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
    }

    private void loadDbGroups(Element root) throws InvocationTargetException, IllegalAccessException {
        NodeList list = root.getElementsByTagName("dbGroup");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Set<String> instanceNames = new HashSet<>();
            Element element = (Element) list.item(i);
            String name = element.getAttribute("name");
            Matcher nameMatcher = PATTERN_DB.matcher(name);
            if (!nameMatcher.matches()) {
                throw new ConfigException("dbGroup name " + name + " show be use " + DB_NAME_FORMAT + "!");
            }
            if (dbGroupConfigs.containsKey(name)) {
                throw new ConfigException("dbGroup name " + name + " duplicated!");
            }
            /*
             * rwSplitMode type for read
             * 1. rwSplitMode="0", read will be send to all writeHost.
             * 2. rwSplitMode="1", read will be send to all readHost
             * 3. rwSplitMode="2", read will be send to all readHost and all writeHost
             */
            final int rwSplitMode = Integer.parseInt(element.getAttribute("rwSplitMode"));

            //slave delay threshold
            String delayThresholdStr = ConfigUtil.checkAndGetAttribute(element, "delayThreshold", "-1", problemReporter);
            final int delayThreshold = Integer.parseInt(delayThresholdStr);
            String disableHAStr = ConfigUtil.checkAndGetAttribute(element, "disableHA", "false", problemReporter);
            boolean disableHA = Boolean.parseBoolean(disableHAStr);

            Element heartbeat = (Element) element.getElementsByTagName("heartbeat").item(0);
            final String heartbeatSQL = heartbeat.getTextContent();
            final String strHBErrorRetryCount = ConfigUtil.checkAndGetAttribute(heartbeat, "errorRetryCount", "1", problemReporter);
            final String strHBTimeout = ConfigUtil.checkAndGetAttribute(heartbeat, "timeout", "0", problemReporter);

            NodeList dbInstances = element.getElementsByTagName("dbInstance");
            int readHostSize = dbInstances.getLength() - 1;
            DbInstanceConfig writeDbConf = null;
            DbInstanceConfig[] readDbConfList = new DbInstanceConfig[readHostSize];
            int readCnt = 0;
            for (int r = 0; r < dbInstances.getLength(); r++) {
                Element dbInstance = (Element) dbInstances.item(r);
                DbInstanceConfig tmpDbInstanceConfig = createDbInstanceConf(name, dbInstance);
                String instanceName = tmpDbInstanceConfig.getInstanceName();
                if (instanceNames.contains(instanceName)) {
                    throw new ConfigException("dbGroup[" + name + "]'s child host name [" + instanceName + "]  duplicated!");
                } else {
                    instanceNames.add(instanceName);
                }
                if (tmpDbInstanceConfig.isPrimary()) {
                    if (writeDbConf == null) {
                        writeDbConf = tmpDbInstanceConfig;
                    } else {
                        throw new ConfigException("dbGroup[" + name + "] has multi primary instance!");
                    }
                } else {
                    if (readCnt == readHostSize) {
                        throw new ConfigException("dbGroup[" + name + "] has no primary instance!");
                    }
                    readDbConfList[readCnt++] = tmpDbInstanceConfig;
                }
            }
            DbGroupConfig dbGroupConf = new DbGroupConfig(name, writeDbConf, readDbConfList, delayThreshold, disableHA);

            dbGroupConf.setRwSplitMode(rwSplitMode);
            dbGroupConf.setHeartbeatSQL(heartbeatSQL);
            dbGroupConf.setHeartbeatTimeout(Integer.parseInt(strHBTimeout) * 1000);
            dbGroupConf.setErrorRetryCount(Integer.parseInt(strHBErrorRetryCount));
            dbGroupConfigs.put(dbGroupConf.getName(), dbGroupConf);
        }
    }

    private DbInstanceConfig createDbInstanceConf(String dbGroup, Element node) throws InvocationTargetException, IllegalAccessException {

        String name = node.getAttribute("name");
        String nodeUrl = node.getAttribute("url");
        String user = node.getAttribute("user");
        String password = node.getAttribute("password");
        Matcher nameMatcher = PATTERN_DB.matcher(name);
        if (!nameMatcher.matches()) {
            throw new ConfigException("dbInstance name " + name + " show be use " + DB_NAME_FORMAT + "!");
        }
        if (StringUtil.isEmpty(name) || StringUtil.isEmpty(nodeUrl) || StringUtil.isEmpty(user)) {
            throw new ConfigException(
                    "dbGroup " + dbGroup +
                            " define error,some attributes of this element is empty: " +
                            name);
        }
        int colonIndex = nodeUrl.indexOf(':');
        String ip = nodeUrl.substring(0, colonIndex).trim();
        int port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
        String usingDecryptStr = ConfigUtil.checkAndGetAttribute(node, "usingDecrypt", "false", problemReporter);
        boolean usingDecrypt = Boolean.parseBoolean(usingDecryptStr);
        password = DecryptUtil.dbHostDecrypt(usingDecrypt, name, user, password);
        String disabledStr = ConfigUtil.checkAndGetAttribute(node, "disabled", "false", problemReporter);
        boolean disabled = Boolean.parseBoolean(disabledStr);
        String primaryStr = ConfigUtil.checkAndGetAttribute(node, "primary", "false", problemReporter);
        boolean primary = Boolean.parseBoolean(primaryStr);

        DbInstanceConfig conf = new DbInstanceConfig(name, ip, port, nodeUrl, user, password, disabled, primary, usingDecrypt);
        // maxCon
        int maxCon = Integer.parseInt(node.getAttribute("maxCon"));
        conf.setMaxCon(maxCon);
        // minCon
        int minCon = Integer.parseInt(node.getAttribute("minCon"));
        conf.setMinCon(minCon);
        // read weight
        String readWeightStr = ConfigUtil.checkAndGetAttribute(node, "readWeight", String.valueOf(PhysicalDbGroup.WEIGHT), problemReporter);
        int readWeight = Integer.parseInt(readWeightStr);
        if (readWeight < 0) {
            throw new ConfigException("readWeight attribute in dbInstance[" + name + "] can't be less than 0!");
        }
        conf.setReadWeight(readWeight);
        // id
        String id = node.getAttribute("id");
        if (StringUtil.isEmpty(id)) {
            conf.setId(name);
        } else {
            conf.setId(id);
        }

        // init properties of connection pool
        PoolConfig poolConfig = new PoolConfig();
        Properties poolProperties = ConfigUtil.loadElements(node);
        ParameterMapping.mapping(poolConfig, poolProperties, problemReporter);
        if (poolProperties.size() > 0) {
            throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(poolProperties.stringPropertyNames(), ","));
        }
        conf.setPoolConfig(poolConfig);

        return conf;
    }

    private Map<String, PhysicalDbGroup> initDbGroups(Map<String, DbGroupConfig> nodeConf) {
        //create PhysicalDBPool according to dbGroup
        Map<String, PhysicalDbGroup> nodes = new LinkedHashMap<>(nodeConf.size());
        for (DbGroupConfig conf : nodeConf.values()) {
            PhysicalDbGroup pool = getPhysicalDBPoolSingleWH(conf);
            nodes.put(pool.getGroupName(), pool);
        }
        return nodes;
    }

    private PhysicalDbInstance createDbInstance(DbGroupConfig conf, DbInstanceConfig node, boolean isRead) {
        return new MySQLInstance(node, conf, isRead);
    }

    private PhysicalDbGroup getPhysicalDBPoolSingleWH(DbGroupConfig conf) {
        //create PhysicalDbInstance for writeDirectly host
        PhysicalDbInstance writeSource = createDbInstance(conf, conf.getWriteInstanceConfig(), false);
        PhysicalDbInstance[] readSources = new PhysicalDbInstance[conf.getReadInstanceConfigs().length];
        int i = 0;
        for (DbInstanceConfig readNode : conf.getReadInstanceConfigs()) {
            readSources[i++] = createDbInstance(conf, readNode, true);
        }

        return new PhysicalDbGroup(conf.getName(), conf, writeSource, readSources, conf.getRwSplitMode());
    }
}
