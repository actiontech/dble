/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.model.*;
import com.actiontech.dble.config.model.TableConfig.TableTypeEnum;
import com.actiontech.dble.config.model.rule.TableRuleConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.SplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author mycat
 */
@SuppressWarnings("unchecked")
public class XMLSchemaLoader implements SchemaLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(XMLSchemaLoader.class);

    private static final String DEFAULT_DTD = "/schema.dtd";
    private static final String DEFAULT_XML = "/schema.xml";

    private final Map<String, TableRuleConfig> tableRules;
    private final Map<String, DataHostConfig> dataHosts;
    private final Map<String, DataNodeConfig> dataNodes;
    private final Map<String, SchemaConfig> schemas;
    private Map<ERTable, Set<ERTable>> erRelations;
    private Map<String, Set<ERTable>> funcNodeERMap;
    private final boolean lowerCaseNames;

    public XMLSchemaLoader(String schemaFile, String ruleFile, boolean lowerCaseNames) {
        //load rule.xml
        XMLRuleLoader ruleLoader = new XMLRuleLoader(ruleFile);
        this.tableRules = ruleLoader.getTableRules();
        this.dataHosts = new HashMap<>();
        this.dataNodes = new HashMap<>();
        this.schemas = new HashMap<>();
        this.lowerCaseNames = lowerCaseNames;
        //load schema
        this.load(DEFAULT_DTD, schemaFile == null ? DEFAULT_XML : schemaFile);
    }

    public XMLSchemaLoader(String schemaFile, String ruleFile) {
        this(schemaFile, ruleFile, true);
    }

    public XMLSchemaLoader() {
        this(true);
    }

    public XMLSchemaLoader(boolean lowerCaseNames) {
        this(null, null, lowerCaseNames);
    }

    @Override
    public Map<String, DataHostConfig> getDataHosts() {
        return (Map<String, DataHostConfig>) (dataHosts.isEmpty() ? Collections.emptyMap() : dataHosts);
    }

    @Override
    public Map<String, DataNodeConfig> getDataNodes() {
        return (Map<String, DataNodeConfig>) (dataNodes.isEmpty() ? Collections.emptyMap() : dataNodes);
    }

    @Override
    public Map<String, SchemaConfig> getSchemas() {
        return (Map<String, SchemaConfig>) (schemas.isEmpty() ? Collections.emptyMap() : schemas);
    }

    @Override
    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    private void load(String dtdFile, String xmlFile) {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream(dtdFile);
            xml = ResourceUtil.getResourceAsStream(xmlFile);
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadDataHosts(root);
            loadDataNodes(root);
            loadSchemas(root);
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

    private void loadSchemas(Element root) {
        NodeList list = root.getElementsByTagName("schema");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element schemaElement = (Element) list.item(i);
            String name = schemaElement.getAttribute("name");
            if (lowerCaseNames) {
                name = name.toLowerCase();
            }
            String dataNode = schemaElement.getAttribute("dataNode");
            String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = -1;
            // sql result size limit
            if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
                sqlMaxLimit = Integer.parseInt(sqlMaxLimitStr);
            }
            //check and add dataNode
            if (dataNode != null && !dataNode.isEmpty()) {
                List<String> dataNodeLst = new ArrayList<>(1);
                dataNodeLst.add(dataNode);
                checkDataNodeExists(dataNodeLst);
            } else {
                dataNode = null;
            }
            //load tables from schema
            Map<String, TableConfig> tables = loadTables(schemaElement, lowerCaseNames);
            if (schemas.containsKey(name)) {
                throw new ConfigException("schema " + name + " duplicated!");
            }

            // if schema has no default dataNode,it must contains at least one table
            if (dataNode == null && tables.size() == 0) {
                throw new ConfigException(
                        "schema " + name + " didn't config tables,so you must set dataNode property!");
            }
            SchemaConfig schemaConfig = new SchemaConfig(name, dataNode,
                    tables, sqlMaxLimit);
            mergeFuncNodeERMap(schemaConfig);
            mergeFkERMap(schemaConfig);
            schemas.put(name, schemaConfig);
        }
        makeAllErRelations();
    }

    private void makeAllErRelations() {
        if (funcNodeERMap == null) {
            return;
        }
        Iterator<Entry<String, Set<ERTable>>> iterator = funcNodeERMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Set<ERTable>> entry = iterator.next();
            if (entry.getValue().size() == 1) {
                iterator.remove();
                continue;
            }
            for (ERTable erTable : entry.getValue()) {
                if (erRelations == null) {
                    erRelations = new HashMap<>();
                }
                Set<ERTable> relations = erRelations.get(erTable);
                if (relations == null) {
                    erRelations.put(erTable, entry.getValue());
                } else {
                    relations.addAll(entry.getValue());
                }
            }
        }
        funcNodeERMap = null;
    }

    private void mergeFuncNodeERMap(SchemaConfig schemaConfig) {
        Map<String, Set<ERTable>> schemaFuncNodeER = schemaConfig.getFuncNodeERMap();
        if (schemaFuncNodeER == null) {
            return;
        }
        for (Entry<String, Set<ERTable>> entry : schemaFuncNodeER.entrySet()) {
            String key = entry.getKey();
            if (funcNodeERMap == null) {
                funcNodeERMap = new HashMap<>();
            }
            if (!funcNodeERMap.containsKey(key)) {
                funcNodeERMap.put(key, entry.getValue());
            } else {
                Set<ERTable> setFuncNode = funcNodeERMap.get(key);
                setFuncNode.addAll(entry.getValue());
            }
        }
        schemaFuncNodeER = null;
    }

    private void mergeFkERMap(SchemaConfig schemaConfig) {
        Map<ERTable, Set<ERTable>> schemaFkERMap = schemaConfig.getFkErRelations();
        if (schemaFkERMap == null) {
            return;
        }
        if (erRelations == null) {
            erRelations = new HashMap<>();
        }
        erRelations.putAll(schemaFkERMap);
    }


    private Map<String, TableConfig> loadTables(Element node, boolean isLowerCaseNames) {
        // supprot [`] BEN GONG in table name
        Map<String, TableConfig> tables = new TableConfigMap();
        NodeList nodeList = node.getElementsByTagName("table");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);

            //primaryKey used for cache and autoincrement
            String primaryKey = tableElement.hasAttribute("primaryKey") ? tableElement.getAttribute("primaryKey").toUpperCase() : null;
            //if autoIncrement,it will use sequence handler
            boolean autoIncrement = false;
            if (tableElement.hasAttribute("autoIncrement")) {
                autoIncrement = Boolean.parseBoolean(tableElement.getAttribute("autoIncrement"));
            }
            //limit size of the table
            boolean needAddLimit = true;
            if (tableElement.hasAttribute("needAddLimit")) {
                needAddLimit = Boolean.parseBoolean(tableElement.getAttribute("needAddLimit"));
            }
            //table type is global or not
            String tableTypeStr = tableElement.hasAttribute("type") ? tableElement.getAttribute("type") : null;
            TableTypeEnum tableType = TableTypeEnum.TYPE_SHARDING_TABLE;
            if ("global".equalsIgnoreCase(tableTypeStr)) {
                tableType = TableTypeEnum.TYPE_GLOBAL_TABLE;
            }
            //dataNode of table
            TableRuleConfig tableRule = null;
            if (tableElement.hasAttribute("rule")) {
                String ruleName = tableElement.getAttribute("rule");
                tableRule = tableRules.get(ruleName);
                if (tableRule == null) {
                    throw new ConfigException("rule " + ruleName + " is not found!");
                }
            }
            boolean ruleRequired = false;
            //ruleRequired?
            if (tableElement.hasAttribute("ruleRequired")) {
                ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
            }
            String dataNode = tableElement.getAttribute("dataNode");
            //distribute function
            String distPrex = "distribute(";
            boolean distTableDns = dataNode.startsWith(distPrex);
            if (distTableDns) {
                dataNode = dataNode.substring(distPrex.length(), dataNode.length() - 1);
            }
            String tableNameElement = tableElement.getAttribute("name");
            if (isLowerCaseNames) {
                tableNameElement = tableNameElement.toLowerCase();
            }
            String[] tableNames = tableNameElement.split(",");
            if (tableNames == null) {
                throw new ConfigException("table name is not found!");
            }
            for (String tableName : tableNames) {
                TableConfig table = new TableConfig(tableName, primaryKey, autoIncrement, needAddLimit, tableType,
                        dataNode, (tableRule != null) ? tableRule.getRule() : null, ruleRequired);
                checkDataNodeExists(table.getDataNodes());
                if (table.getRule() != null) {
                    checkRuleSuitTable(table);
                }
                if (distTableDns) {
                    distributeDataNodes(table.getDataNodes());
                }
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                tables.put(table.getName(), table);
            }
            // child table must know its unique father
            if (tableNames.length == 1) {
                TableConfig parentTable = tables.get(tableNames[0]);
                // process child tables
                processChildTables(tables, parentTable, dataNode, tableElement, isLowerCaseNames);
            }
        }
        return tables;
    }

    /**
     * distribute data nodes in multi hosts, reorder data node according to host .
     * eg :dn1 (host1),dn2(host1),dn100(host2),dn101(host2),dn300(host3),dn101(host2),dn301(host3)...etc
     * result: 0->dn1 (host1),1->dn100(host2),2->dn300(host3),3->dn2(host1),4->dn101(host2),5->dn301(host3)
     *
     * @param theDataNodes
     */
    private void distributeDataNodes(ArrayList<String> theDataNodes) {
        Map<String, ArrayList<String>> newDataNodeMap = new HashMap<>(dataHosts.size());
        for (String dn : theDataNodes) {
            DataNodeConfig dnConf = dataNodes.get(dn);
            String host = dnConf.getDataHost();
            ArrayList<String> hostDns = newDataNodeMap.get(host);
            hostDns = (hostDns == null) ? new ArrayList<String>() : hostDns;
            hostDns.add(dn);
            newDataNodeMap.put(host, hostDns);
        }

        ArrayList<String> result = new ArrayList<>(theDataNodes.size());
        boolean hasData = true;
        while (hasData) {
            hasData = false;
            for (ArrayList<String> dns : newDataNodeMap.values()) {
                if (!dns.isEmpty()) {
                    result.add(dns.remove(0));
                    hasData = true;
                }
            }
        }
        theDataNodes.clear();
        theDataNodes.addAll(result);
    }

    private void processChildTables(Map<String, TableConfig> tables,
                                    TableConfig parentTable, String strDatoNodes, Element tableNode, boolean isLowerCaseNames) {

        // parse child tables
        NodeList childNodeList = tableNode.getChildNodes();
        for (int j = 0; j < childNodeList.getLength(); j++) {
            Node theNode = childNodeList.item(j);
            if (!theNode.getNodeName().equals("childTable")) {
                continue;
            }
            Element childTbElement = (Element) theNode;
            String cdTbName = childTbElement.getAttribute("name");
            if (isLowerCaseNames) {
                cdTbName = cdTbName.toLowerCase();
            }
            String primaryKey = childTbElement.hasAttribute("primaryKey") ? childTbElement.getAttribute("primaryKey").toUpperCase() : null;

            boolean autoIncrement = false;
            if (childTbElement.hasAttribute("autoIncrement")) {
                autoIncrement = Boolean.parseBoolean(childTbElement.getAttribute("autoIncrement"));
            }
            boolean needAddLimit = true;
            if (childTbElement.hasAttribute("needAddLimit")) {
                needAddLimit = Boolean.parseBoolean(childTbElement.getAttribute("needAddLimit"));
            }
            //join key ,the parent's column
            String joinKey = childTbElement.getAttribute("joinKey").toUpperCase();
            String parentKey = childTbElement.getAttribute("parentKey").toUpperCase();
            TableConfig table = new TableConfig(cdTbName, primaryKey, autoIncrement, needAddLimit,
                    TableTypeEnum.TYPE_SHARDING_TABLE, strDatoNodes, null, false, parentTable, joinKey, parentKey);

            if (tables.containsKey(table.getName())) {
                throw new ConfigException("table " + table.getName() + " duplicated!");
            }
            tables.put(table.getName(), table);
            //child table may also have children
            processChildTables(tables, table, strDatoNodes, childTbElement, isLowerCaseNames);
        }
    }

    private void checkDataNodeExists(Collection<String> nodes) {
        if (nodes == null || nodes.size() < 1) {
            return;
        }
        for (String node : nodes) {
            if (!dataNodes.containsKey(node)) {
                throw new ConfigException("dataNode '" + node + "' is not found!");
            }
        }
    }

    /**
     * shard table datanode(2) < function count(3) and check failed
     */
    private void checkRuleSuitTable(TableConfig tableConf) {
        AbstractPartitionAlgorithm function = tableConf.getRule().getRuleAlgorithm();
        int suitValue = function.suitableFor(tableConf);
        if (suitValue < 0) {
            throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ " +
                    tableConf.getRule().getFunctionName() + " ] partition size : " + tableConf.getRule().getRuleAlgorithm().getPartitionNum() + " > table datanode size : " +
                    tableConf.getDataNodes().size() + ", please make sure table datanode size = function partition size");
        } else if (suitValue > 0) {
            LOGGER.warn("table conf : table [ {} ] rule function [ {} ] partition size : {} < table datanode size : {} , this cause some datanode to be redundant",
                    new String[]{
                            tableConf.getName(),
                            tableConf.getRule().getFunctionName(),
                            String.valueOf(tableConf.getRule().getRuleAlgorithm().getPartitionNum()),
                            String.valueOf(tableConf.getDataNodes().size()),
                    });

        } else {
            // table data node size == rule function partition size
        }
    }

    private void loadDataNodes(Element root) {
        NodeList list = root.getElementsByTagName("dataNode");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element element = (Element) list.item(i);
            String dnNamePre = element.getAttribute("name");

            String databaseStr = element.getAttribute("database");
            if (lowerCaseNames) {
                databaseStr = databaseStr.toLowerCase();
            }
            String host = element.getAttribute("dataHost");
            if (empty(dnNamePre) || empty(databaseStr) || empty(host)) {
                throw new ConfigException("dataNode " + dnNamePre + " define error ,attribute can't be empty");
            }
            //dnNamePre(name),databaseStr(database),host(dataHost) can use ',', '$', '-' to configure multi nodes
            // but the database size *dataHost size must equal the size of name
            // every dataHost has all database in its tag
            //eg:<dataNode name="dn1$0-75" dataHost="localhost$1-10" database="db$0-75" />
            //means:localhost1 has database of dn1$0-75,localhost2has database of dn1$0-75(name is db$76-151)
            String[] dnNames = SplitUtil.split(dnNamePre, ',', '$', '-');
            String[] databases = SplitUtil.split(databaseStr, ',', '$', '-');
            String[] hostStrings = SplitUtil.split(host, ',', '$', '-');

            if (dnNames.length > 1 && dnNames.length != databases.length * hostStrings.length) {
                throw new ConfigException("dataNode " + dnNamePre +
                        " define error ,dnNames.length must be=databases.length*hostStrings.length");
            }
            if (dnNames.length > 1) {

                List<String[]> mhdList = mergerHostDatabase(hostStrings, databases);
                for (int k = 0; k < dnNames.length; k++) {
                    String[] hd = mhdList.get(k);
                    String dnName = dnNames[k];
                    String databaseName = hd[1];
                    String hostName = hd[0];
                    createDataNode(dnName, databaseName, hostName);
                }

            } else {
                createDataNode(dnNamePre, databaseStr, host);
            }

        }
    }

    /**
     * @param hostStrings
     * @param databases
     * @return
     */
    private List<String[]> mergerHostDatabase(String[] hostStrings, String[] databases) {
        List<String[]> mhdList = new ArrayList<>();
        for (String hostString : hostStrings) {
            for (String database : databases) {
                String[] hd = new String[2];
                hd[0] = hostString;
                hd[1] = database;
                mhdList.add(hd);
            }
        }
        return mhdList;
    }

    private void createDataNode(String dnName, String database, String host) {

        DataNodeConfig conf = new DataNodeConfig(dnName, database, host);
        if (dataNodes.containsKey(conf.getName())) {
            throw new ConfigException("dataNode " + conf.getName() + " duplicated!");
        }

        if (!dataHosts.containsKey(host)) {
            throw new ConfigException("dataNode " + dnName + " reference dataHost:" + host + " not exists!");
        }
        dataNodes.put(conf.getName(), conf);
    }

    private boolean empty(String dnName) {
        return dnName == null || dnName.length() == 0;
    }

    private DBHostConfig createDBHostConf(String dataHost, Element node, int maxCon, int minCon) {

        String nodeHost = node.getAttribute("host");
        String nodeUrl = node.getAttribute("url");
        String user = node.getAttribute("user");

        String ip = null;
        int port = 0;
        if (empty(nodeHost) || empty(nodeUrl) || empty(user)) {
            throw new ConfigException(
                    "dataHost " + dataHost +
                            " define error,some attributes of this element is empty: " +
                            nodeHost);
        }

        int colonIndex = nodeUrl.indexOf(':');
        ip = nodeUrl.substring(0, colonIndex).trim();
        port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());

        String password = node.getAttribute("password");
        String usingDecrypt = node.getAttribute("usingDecrypt");
        String passwordEncryty = DecryptUtil.dbHostDecrypt(usingDecrypt, nodeHost, user, password);
        DBHostConfig conf = new DBHostConfig(nodeHost, ip, port, nodeUrl, user, passwordEncryty);
        conf.setMaxCon(maxCon);
        conf.setMinCon(minCon);

        String weightStr = node.getAttribute("weight");
        int weight = "".equals(weightStr) ? PhysicalDBPool.WEIGHT : Integer.parseInt(weightStr);
        conf.setWeight(weight);
        return conf;
    }

    private void loadDataHosts(Element root) {
        NodeList list = root.getElementsByTagName("dataHost");
        for (int i = 0, n = list.getLength(); i < n; ++i) {

            Element element = (Element) list.item(i);
            String name = element.getAttribute("name");
            if (dataHosts.containsKey(name)) {
                throw new ConfigException("dataHost name " + name + "duplicated!");
            }
            int maxCon = Integer.parseInt(element.getAttribute("maxCon"));
            int minCon = Integer.parseInt(element.getAttribute("minCon"));
            /**
             * balance type for read
             * 1. balance="0", read will be send to all writeHost.
             * 2. balance="1", read will be send to all readHost and stand by writeHost
             * 3. balance="2", read will be send to all readHost and all writeHost
             * 4. balance="3", read will be send to all readHost
             */
            final int balance = Integer.parseInt(element.getAttribute("balance"));
            /**
             * switchType
             * -1 No switch
             * 1 switch automatically
             * 2 switch according to the second behind of slave ,the heartbeat query is show slave status
             * 3 switch according to Galera Cluster for MySQL
             */
            String switchTypeStr = element.getAttribute("switchType");
            int switchType = switchTypeStr.equals("") ? -1 : Integer.parseInt(switchTypeStr);
            //slave delay threshold
            String slaveThresholdStr = element.getAttribute("slaveThreshold");
            int slaveThreshold = slaveThresholdStr.equals("") ? -1 : Integer.parseInt(slaveThresholdStr);

            //tempReadHostAvailable >0 means read service is still work even write host crash
            String tempReadHostAvailableStr = element.getAttribute("tempReadHostAvailable");
            boolean tempReadHostAvailable = !tempReadHostAvailableStr.equals("") && Integer.parseInt(tempReadHostAvailableStr) > 0;

            final String heartbeatSQL = element.getElementsByTagName("heartbeat").item(0).getTextContent();

            NodeList writeNodes = element.getElementsByTagName("writeHost");
            DBHostConfig[] writeDbConfs = new DBHostConfig[writeNodes.getLength()];
            Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<>(2);
            for (int w = 0; w < writeDbConfs.length; w++) {
                Element writeNode = (Element) writeNodes.item(w);
                writeDbConfs[w] = createDBHostConf(name, writeNode, maxCon, minCon);
                NodeList readNodes = writeNode.getElementsByTagName("readHost");
                //for every readHost
                if (readNodes.getLength() != 0) {
                    DBHostConfig[] readDbConfs = new DBHostConfig[readNodes.getLength()];
                    for (int r = 0; r < readDbConfs.length; r++) {
                        Element readNode = (Element) readNodes.item(r);
                        readDbConfs[r] = createDBHostConf(name, readNode, maxCon, minCon);
                    }
                    readHostsMap.put(w, readDbConfs);
                }
            }

            DataHostConfig hostConf = new DataHostConfig(name,
                    writeDbConfs, readHostsMap, switchType, slaveThreshold, tempReadHostAvailable);

            hostConf.setMaxCon(maxCon);
            hostConf.setMinCon(minCon);
            hostConf.setBalance(balance);
            hostConf.setHearbeatSQL(heartbeatSQL);
            dataHosts.put(hostConf.getName(), hostConf);
        }
    }

}
