/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.ShardingNodeConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.route.function.*;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import static com.actiontech.dble.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_DEFAULT;
import static com.actiontech.dble.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_DEFAULT_CRON;

@SuppressWarnings("unchecked")
public class XMLShardingLoader {
    private static final String DEFAULT_DTD = "/sharding.dtd";
    private static final String DEFAULT_XML = "/" + ConfigFileName.SHARDING_XML;

    private final Map<String, ShardingNodeConfig> shardingNode;
    private final Map<String, SchemaConfig> schemas;
    private Map<ERTable, Set<ERTable>> erRelations;
    private Map<String, Set<ERTable>> funcNodeERMap;
    private final Map<String, AbstractPartitionAlgorithm> functions;
    private final boolean lowerCaseNames;
    private ProblemReporter problemReporter;
    private AtomicInteger tableIndex = new AtomicInteger();

    public XMLShardingLoader(String shardingFile, boolean lowerCaseNames, ProblemReporter problemReporter) {
        this.functions = new LinkedHashMap<>();
        this.shardingNode = new HashMap<>();
        this.schemas = new HashMap<>();
        this.lowerCaseNames = lowerCaseNames;
        this.problemReporter = problemReporter;
        //load sharding
        this.load(DEFAULT_DTD, shardingFile == null ? DEFAULT_XML : shardingFile);
    }

    public XMLShardingLoader(boolean lowerCaseNames, ProblemReporter problemReporter) {
        this(null, lowerCaseNames, problemReporter);
    }

    public int addTableIndex() {
        return tableIndex.incrementAndGet();
    }

    public Map<String, ShardingNodeConfig> getShardingNode() {
        return (Map<String, ShardingNodeConfig>) (shardingNode.isEmpty() ? Collections.emptyMap() : shardingNode);
    }

    public Map<String, AbstractPartitionAlgorithm> getFunctions() {
        return functions;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return (Map<String, SchemaConfig>) (schemas.isEmpty() ? Collections.emptyMap() : schemas);
    }

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
            String version = null;
            if (root.getAttributes().getNamedItem("version") != null) {
                version = root.getAttributes().getNamedItem("version").getNodeValue();
            }
            if (version != null && !Versions.CONFIG_VERSION.equals(version)) {
                if (this.problemReporter != null) {
                    if (Versions.checkVersion(version)) {
                        String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.SHARDING_XML + " version is " + version + ".There may be some incompatible config between two versions, please check it";
                        this.problemReporter.warn(message);
                    } else {
                        String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.SHARDING_XML + " version is " + version + ".There must be some incompatible config between two versions, please check it";
                        this.problemReporter.warn(message);
                    }
                }
            }
            loadShardingNode(root);
            loadFunctions(root);
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
            String shardingNodes = schemaElement.getAttribute("shardingNode");
            String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = getSqlMaxLimit(sqlMaxLimitStr, -1);
            //check and add shardingNode
            if (shardingNodes != null && !shardingNodes.isEmpty()) {
                List<String> shardingNodeLst = new ArrayList<>(1);
                shardingNodeLst.add(shardingNodes);
                checkShardingNodeExists(shardingNodeLst);
            } else {
                shardingNodes = null;
            }
            //load tables from sharding
            Map<String, BaseTableConfig> tables = loadTables(schemaElement, lowerCaseNames, sqlMaxLimit);
            if (schemas.containsKey(name)) {
                throw new ConfigException("schema " + name + " duplicated!");
            }

            // if sharding has no default shardingNode,it must contains at least one table
            if (shardingNodes == null && tables.size() == 0) {
                throw new ConfigException(
                        "sharding " + name + " didn't config tables,so you must set shardingNode property!");
            }
            SchemaConfig schemaConfig = new SchemaConfig(name, shardingNodes, tables, sqlMaxLimit);
            mergeFuncNodeERMap(schemaConfig);
            mergeFkERMap(schemaConfig);
            schemas.put(name, schemaConfig);
        }
        makeAllErRelations();
    }

    private int getSqlMaxLimit(String sqlMaxLimitStr, int defaultMaxLimit) {
        // sql result size limit
        if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
            defaultMaxLimit = Integer.parseInt(sqlMaxLimitStr);
            if (defaultMaxLimit < -1) {
                defaultMaxLimit = -1;
            }
        }
        return defaultMaxLimit;
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

    private Map<String, BaseTableConfig> loadTables(Element node, boolean isLowerCaseNames, int schemaMaxLimit) {
        Map<String, BaseTableConfig> tables = new HashMap<>();
        loadShardingTables(node, isLowerCaseNames, tables, schemaMaxLimit);
        loadGlobalTables(node, isLowerCaseNames, tables, schemaMaxLimit);
        loadSingleTables(node, isLowerCaseNames, tables, schemaMaxLimit);
        return tables;
    }

    private void loadShardingTables(Element node, boolean isLowerCaseNames, Map<String, BaseTableConfig> tables, int schemaMaxLimit) {
        NodeList nodeList = node.getElementsByTagName("shardingTable");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String tableNameElement = tableElement.getAttribute("name");
            if (StringUtil.isEmpty(tableNameElement)) {
                throw new ConfigException("one of tables' name is empty");
            }
            String sqlMaxLimitStr = tableElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = getSqlMaxLimit(sqlMaxLimitStr, schemaMaxLimit);

            //shardingNode of table
            String column = tableElement.getAttribute("shardingColumn");
            if (StringUtil.isEmpty(column)) {
                throw new ConfigException("shardingColumn of " + tableNameElement + " is empty");
            }
            column = column.toUpperCase();
            String functionName = tableElement.getAttribute("function");

            if (StringUtil.isEmpty(functionName)) {
                throw new ConfigException("function of " + tableNameElement + " is empty");
            }
            AbstractPartitionAlgorithm algorithm = functions.get(functionName);
            if (algorithm == null) {
                throw new ConfigException("can't find function of name :" + functionName + " in table " + tableNameElement);
            }

            //sqlRequiredSharding?
            String sqlRequiredShardingStr = ConfigUtil.checkAndGetAttribute(tableElement, "sqlRequiredSharding", "false", problemReporter);
            boolean sqlRequiredSharding = Boolean.parseBoolean(sqlRequiredShardingStr);

            String shardingNodeRef = tableElement.getAttribute("shardingNode");
            if (StringUtil.isEmpty(shardingNodeRef)) {
                throw new ConfigException("shardingNode of " + tableNameElement + " is empty");
            }
            String[] theShardingNodes = SplitUtil.split(shardingNodeRef, ',', '$', '-');
            if (theShardingNodes.length <= 1) {
                throw new ConfigException("invalid shardingNode config: " + shardingNodeRef + " for ShardingTableConfig " + tableNameElement + ", please use SingleTable");
            }
            List<String> lstShardingNode = Arrays.asList(theShardingNodes);
            if (isLowerCaseNames) {
                tableNameElement = tableNameElement.toLowerCase();
            }
            String[] tableNames = tableNameElement.split(",");

            //if autoIncrement,it will use sequence handler
            String incrementColumn = tableElement.hasAttribute("incrementColumn") ? tableElement.getAttribute("incrementColumn").toUpperCase() : null;
            for (String tableName : tableNames) {
                if (tableName.contains("`")) {
                    tableName = tableName.replaceAll("`", "");
                }
                if (StringUtil.isEmpty(tableName)) {
                    throw new ConfigException("one of table name of " + tableNameElement + " is empty");
                }
                ShardingTableConfig table = new ShardingTableConfig(tableName, sqlMaxLimit,
                        lstShardingNode, incrementColumn, algorithm, column, sqlRequiredSharding);
                checkShardingNodeExists(table.getShardingNodes());
                checkRuleSuitTable(table, functionName);
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                table.setId(addTableIndex());
                tables.put(table.getName(), table);
            }
            // child table must know its unique father
            if (tableNames.length == 1) {
                ShardingTableConfig parentTable = (ShardingTableConfig) (tables.get(tableNames[0]));
                // process child tables
                processChildTables(tables, parentTable, lstShardingNode, tableElement, isLowerCaseNames, schemaMaxLimit);
            }
        }
    }



    private void loadGlobalTables(Element node, boolean isLowerCaseNames, Map<String, BaseTableConfig> tables, int schemaMaxLimit) {
        NodeList nodeList = node.getElementsByTagName("globalTable");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String tableNameElement = tableElement.getAttribute("name");
            if (StringUtil.isEmpty(tableNameElement)) {
                throw new ConfigException("one of tables' name is empty");
            }
            //limit size of the table
            String sqlMaxLimitStr = tableElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = getSqlMaxLimit(sqlMaxLimitStr, schemaMaxLimit);
            String shardingNodeRef = tableElement.getAttribute("shardingNode");
            if (StringUtil.isEmpty(shardingNodeRef)) {
                throw new ConfigException("shardingNode of " + tableNameElement + " is empty");
            }
            String[] theShardingNodes = SplitUtil.split(shardingNodeRef, ',', '$', '-');
            if (theShardingNodes.length <= 1) {
                throw new ConfigException("invalid shardingNode config: " + shardingNodeRef + " for GlobalTableConfig " + tableNameElement + ", please use SingleTable");
            }
            if (isLowerCaseNames) {
                tableNameElement = tableNameElement.toLowerCase();
            }
            String[] tableNames = tableNameElement.split(",");

            boolean globalCheck = false;
            String checkClass = GLOBAL_TABLE_CHECK_DEFAULT;
            String corn = GLOBAL_TABLE_CHECK_DEFAULT_CRON;
            if (tableElement.hasAttribute("checkClass")) {
                checkClass = tableElement.getAttribute("checkClass");
                corn = tableElement.hasAttribute("cron") ? tableElement.getAttribute("cron").toUpperCase() : GLOBAL_TABLE_CHECK_DEFAULT_CRON;
                globalCheck = true;
            }

            for (String tableName : tableNames) {
                if (tableName.contains("`")) {
                    tableName = tableName.replaceAll("`", "");
                }
                if (StringUtil.isEmpty(tableName)) {
                    throw new ConfigException("one of table name of " + tableNameElement + " is empty");
                }
                GlobalTableConfig table = new GlobalTableConfig(tableName, sqlMaxLimit, Arrays.asList(theShardingNodes),
                        corn, checkClass, globalCheck);
                checkShardingNodeExists(table.getShardingNodes());
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                table.setId(addTableIndex());
                tables.put(table.getName(), table);
            }
        }
    }

    private void loadSingleTables(Element node, boolean isLowerCaseNames, Map<String, BaseTableConfig> tables, int schemaMaxLimit) {
        NodeList nodeList = node.getElementsByTagName("singleTable");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String tableNameElement = tableElement.getAttribute("name");
            if (StringUtil.isEmpty(tableNameElement)) {
                throw new ConfigException("one of tables' name is empty");
            }
            //limit size of the table
            String sqlMaxLimitStr = tableElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = getSqlMaxLimit(sqlMaxLimitStr, schemaMaxLimit);
            String shardingNodeRef = tableElement.getAttribute("shardingNode");
            if (StringUtil.isEmpty(shardingNodeRef)) {
                throw new ConfigException("shardingNode of " + tableNameElement + " is empty");
            }
            String[] theShardingNodes = SplitUtil.split(shardingNodeRef, ',', '$', '-');
            if (theShardingNodes.length != 1) {
                throw new ConfigException("invalid shardingNode config: " + shardingNodeRef + " for SingleTableConfig " + tableNameElement);
            }
            if (isLowerCaseNames) {
                tableNameElement = tableNameElement.toLowerCase();
            }
            String[] tableNames = tableNameElement.split(",");

            for (String tableName : tableNames) {
                if (tableName.contains("`")) {
                    tableName = tableName.replaceAll("`", "");
                }
                if (StringUtil.isEmpty(tableName)) {
                    throw new ConfigException("one of table name of " + tableNameElement + " is empty");
                }
                SingleTableConfig table = new SingleTableConfig(tableName, sqlMaxLimit, Arrays.asList(theShardingNodes));
                checkShardingNodeExists(table.getShardingNodes());
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                table.setId(addTableIndex());
                tables.put(table.getName(), table);
            }
        }
    }


    private void processChildTables(Map<String, BaseTableConfig> tables, BaseTableConfig parentTable, List<String> lstShardingNode,
                                    Element tableNode, boolean isLowerCaseNames, int schemaMaxLimit) {

        // parse child tables
        NodeList childNodeList = tableNode.getChildNodes();
        for (int j = 0; j < childNodeList.getLength(); j++) {
            Node theNode = childNodeList.item(j);
            if (!theNode.getNodeName().equals("childTable")) {
                continue;
            }
            Element childTbElement = (Element) theNode;
            String tableName = childTbElement.getAttribute("name");
            if (StringUtil.isEmpty(tableName)) {
                throw new ConfigException("one of table [" + parentTable.getName() + "]'s child name is empty");
            }
            if (isLowerCaseNames) {
                tableName = tableName.toLowerCase();
            }

            String sqlMaxLimitStr = childTbElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = getSqlMaxLimit(sqlMaxLimitStr, schemaMaxLimit);

            //join key ,the parent's column
            String joinColumn = childTbElement.getAttribute("joinColumn").toUpperCase();
            String parentColumn = childTbElement.getAttribute("parentColumn").toUpperCase();
            //if autoIncrement,it will use sequence handler
            String incrementColumn = childTbElement.hasAttribute("incrementColumn") ? childTbElement.getAttribute("incrementColumn").toUpperCase() : null;
            ChildTableConfig table = new ChildTableConfig(tableName, sqlMaxLimit, lstShardingNode,
                    parentTable, joinColumn, parentColumn, incrementColumn);

            if (tables.containsKey(table.getName())) {
                throw new ConfigException("table " + table.getName() + " duplicated!");
            }
            table.setId(addTableIndex());
            tables.put(table.getName(), table);
            //child table may also have children
            processChildTables(tables, table, lstShardingNode, childTbElement, isLowerCaseNames, schemaMaxLimit);
        }
    }

    private void checkShardingNodeExists(Collection<String> nodes) {
        if (nodes == null || nodes.size() < 1) {
            return;
        }
        for (String node : nodes) {
            if (!shardingNode.containsKey(node)) {
                throw new ConfigException("shardingNode '" + node + "' is not found!");
            }
        }
    }

    /**
     * shard table shardingnode(2) < function count(3) and check failed
     */
    private void checkRuleSuitTable(ShardingTableConfig tableConf, String functionName) {
        AbstractPartitionAlgorithm function = tableConf.getFunction();
        int suitValue = function.suitableFor(tableConf.getShardingNodes().size());
        if (suitValue < 0) {
            throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ " +
                    functionName + " ] partition size : " + tableConf.getShardingColumn() + " > table shardingNode size : " +
                    tableConf.getShardingNodes().size() + ", please make sure table shardingnode size = function partition size");
        } else if (suitValue > 0) {
            problemReporter.warn("table conf : table [ " + tableConf.getName() + " ] rule function [ " + functionName + " ] " +
                    "partition size : " + String.valueOf(tableConf.getFunction().getPartitionNum()) + " < table shardingNode size : " + String.valueOf(tableConf.getShardingNodes().size()));
        }
        // else {
        // table shardingNode size == rule function partition size
        //}
    }

    private void loadShardingNode(Element root) {
        NodeList list = root.getElementsByTagName("shardingNode");
        Set<String> checkSet = new HashSet<>();
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element element = (Element) list.item(i);
            String dnNamePre = element.getAttribute("name");
            String databaseStr = element.getAttribute("database");
            if (lowerCaseNames) {
                databaseStr = databaseStr.toLowerCase();
            }
            String host = element.getAttribute("dbGroup");
            if (StringUtils.isBlank(dnNamePre) || StringUtils.isBlank(databaseStr) || StringUtils.isBlank(host)) {
                throw new ConfigException("shardingNode " + dnNamePre + " define error ,attribute can't be empty");
            }
            //dnNamePre(name),databaseStr(database),host(dbGroup) can use ',', '$', '-' to configure multi nodes
            // but the database size *dbGroup size must equal the size of name
            // every dbGroup has all database in its tag
            //eg:<shardingNode name="dn$0-759" dbGroup="localhost$1-10" database="db$0-75" />
            //means:localhost1 has database of db$0-75,localhost2 has database of db$0-75(name is dn$76-151)
            String[] dnNames = SplitUtil.split(dnNamePre, ',', '$', '-');
            String[] databases = SplitUtil.split(databaseStr, ',', '$', '-');
            String[] hostStrings = SplitUtil.split(host, ',', '$', '-');

            if (dnNames.length != databases.length * hostStrings.length) {
                throw new ConfigException("shardingNode " + dnNamePre +
                        " define error ,Number of shardingNode name must be = Number of database * Number of dbGroup");
            }
            if (dnNames.length > 1) {
                List<String[]> mhdList = mergerHostDatabase(hostStrings, databases);
                for (int k = 0; k < dnNames.length; k++) {
                    String[] hd = mhdList.get(k);
                    String dnName = dnNames[k];
                    String databaseName = hd[1];
                    String hostName = hd[0];
                    createSharingNode(dnName, databaseName, hostName, checkSet);
                }
            } else {
                createSharingNode(dnNamePre, databaseStr, host, checkSet);
            }
        }
    }

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

    private void createSharingNode(String dnName, String database, String host, Set checkSet) {

        ShardingNodeConfig conf = new ShardingNodeConfig(dnName, database, host);
        if (checkSet.contains(host + "#" + database)) {
            throw new ConfigException("shardingNode " + conf.getName() + " use the same dbGroup&database with other shardingNode");
        } else {
            checkSet.add(host + "#" + database);
        }
        if (shardingNode.containsKey(conf.getName())) {
            throw new ConfigException("shardingNode " + conf.getName() + " duplicated!");
        }
        shardingNode.put(conf.getName(), conf);
    }
    private void loadFunctions(Element root) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        NodeList list = root.getElementsByTagName("function");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                // check if the function is duplicate
                if (functions.containsKey(name)) {
                    throw new ConfigException("rule function " + name + " duplicated!");
                }
                String clazz = e.getAttribute("class");
                //reflection
                AbstractPartitionAlgorithm function = createFunction(name, clazz);
                function.setName(name);
                Properties props = ConfigUtil.loadElements(e);
                ParameterMapping.mapping(function, props, problemReporter);
                if (props.size() > 0) {
                    String[] propItem = new String[props.size()];
                    props.keySet().toArray(propItem);
                    throw new ConfigException("These properties of function [" + name + "] is not recognized: " + StringUtil.join(propItem, ","));
                }
                //init for AbstractPartitionAlgorithm
                function.selfCheck();
                function.init();
                functions.put(name, function);
            }
        }
        setFunctionAlias();
    }

    private void setFunctionAlias() {
        Map<AbstractPartitionAlgorithm, String> funcAlias = new HashMap<>();
        int i = 0;
        for (AbstractPartitionAlgorithm function : functions.values()) {
            String alias = funcAlias.get(function);
            if (alias != null) {
                function.setAlias(alias);
            } else {
                alias = "function" + i;
                i++;
                function.setAlias(alias);
                funcAlias.put(function, alias);
            }
        }
    }

    private AbstractPartitionAlgorithm createFunction(String name, String clazz)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException {

        String lowerClass = clazz.toLowerCase();
        switch (lowerClass) {
            case "hash":
                return new PartitionByLong();
            case "stringhash":
                return new PartitionByString();
            case "enum":
                return new PartitionByFileMap();
            case "jumpstringhash":
                return new PartitionByJumpConsistentHash();
            case "numberrange":
                return new AutoPartitionByLong();
            case "patternrange":
                return new PartitionByPattern();
            case "date":
                return new PartitionByDate();
            default:
                Class<?> clz = Class.forName(clazz);
                //all function must be extend from AbstractPartitionAlgorithm
                if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
                    throw new ConfigException("rule function must implements " +
                            AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
                }
                return (AbstractPartitionAlgorithm) clz.newInstance();
        }

    }
}
