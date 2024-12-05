/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config.converter;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.JsonObjectWriter;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.ConfFileRWUtils;
import com.oceanbase.obsharding_d.cluster.zkprocess.console.ParseParamEnum;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.Property;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.Shardings;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.function.Function;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.schema.*;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.shardingnode.ShardingNode;
import com.oceanbase.obsharding_d.cluster.zkprocess.parse.XmlProcessBase;
import com.oceanbase.obsharding_d.config.ConfigFileName;
import com.oceanbase.obsharding_d.config.ErrorInfo;
import com.oceanbase.obsharding_d.config.ProblemReporter;
import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.config.model.db.type.DataBaseType;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.ShardingNodeConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.*;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.config.util.ParameterMapping;
import com.oceanbase.obsharding_d.route.function.*;
import com.oceanbase.obsharding_d.singleton.SequenceManager;
import com.oceanbase.obsharding_d.util.SplitUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oceanbase.obsharding_d.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_DEFAULT;
import static com.oceanbase.obsharding_d.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_DEFAULT_CRON;

public class ShardingConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingConverter.class);

    private Map<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode> shardingNodeMap = Maps.newLinkedHashMap();
    private final Map<String, AbstractPartitionAlgorithm> functionMap = Maps.newLinkedHashMap();
    private final Map<String, SchemaConfig> schemaConfigMap = Maps.newLinkedHashMap();
    private final Map<ERTable, Set<ERTable>> erRelations = Maps.newLinkedHashMap();
    private final Map<String, Set<ERTable>> funcNodeERMap = Maps.newLinkedHashMap();
    private final AtomicInteger tableIndex = new AtomicInteger(0);
    private final Gson gson = JsonFactory.getJson();

    public ShardingConverter() {
    }

    public RawJson shardingXmlToJson() throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Shardings.class);
        xmlProcess.initJaxbClass();
        return parseShardingXmlFileToJson(xmlProcess);
    }

    public RawJson shardingXmlToJson(String xmlPath) throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Shardings.class);
        xmlProcess.initJaxbClass();
        return parseShardingXmlFileToJson(xmlProcess, xmlPath, ConfigFileName.SHARDING_XSD);
    }

    public Shardings shardingJsonToBean(RawJson shardingJson) {
        return ClusterLogic.forConfig().parseShardingJsonToBean(gson, shardingJson);
    }

    public RawJson shardingBeanToJson(Shardings shardings) {
        // bean to json obj
        JsonObjectWriter jsonObj = new JsonObjectWriter();
        jsonObj.addProperty(ClusterPathUtil.VERSION, shardings.getVersion());

        JsonArray schemaArray = new JsonArray();
        for (Schema schema : shardings.getSchema()) {
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
        jsonObj.add(ClusterPathUtil.SHARDING_NODE, gson.toJsonTree(shardings.getShardingNode()));
        List<Function> functionList = shardings.getFunction();
        readMapFileAddFunction(functionList);
        jsonObj.add(ClusterPathUtil.FUNCTION, gson.toJsonTree(functionList));
        return RawJson.of(jsonObj);
    }

    public void shardingJsonToMap(RawJson shardingJson, Map<String, PhysicalDbGroup> dbGroupMap, RawJson sequenceJson, ProblemReporter problemReporter) {
        Shardings shardings = shardingJsonToBean(shardingJson);
        List<ShardingNode> shardingNodeList = shardings.getShardingNode();
        List<Function> functionList = shardings.getFunction();
        removeFileContent(functionList);
        List<Schema> schemaList = shardings.getSchema();
        Map<String, ShardingNodeConfig> shardingNodeConfigMap = Maps.newLinkedHashMap();
        List<ErrorInfo> errorInfos = new ArrayList<>();
        if (shardings.getVersion() != null && !Versions.CONFIG_VERSION.equals(shardings.getVersion())) {
            if (problemReporter != null) {
                if (Versions.checkVersion(shardings.getVersion())) {
                    String message = "The OBsharding-D-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.SHARDING_XML + " version is " + shardings.getVersion() + ".There may be some incompatible config between two versions, please check it";
                    problemReporter.warn(message);
                } else {
                    String message = "The OBsharding-D-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.SHARDING_XML + " version is " + shardings.getVersion() + ".There must be some incompatible config between two versions, please check it";
                    problemReporter.warn(message);
                }
            }
        }
        try {
            shardingNodeListToMap(shardingNodeList, dbGroupMap, shardingNodeConfigMap);
            functionListToMap(functionList, problemReporter);
            schemaListToMap(schemaList, shardingNodeConfigMap, problemReporter);
            deleteUselessShardingNode(errorInfos, sequenceJson);
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            throw new ConfigException("sharding json to map occurred  parse errors, The detailed errors are as follows. " + message, e);
        }
    }

    RawJson parseShardingXmlFileToJson(XmlProcessBase xmlParseBase) throws Exception {
        return parseShardingXmlFileToJson(xmlParseBase, ConfigFileName.SHARDING_XML, ConfigFileName.SHARDING_XSD);
    }

    RawJson parseShardingXmlFileToJson(XmlProcessBase xmlParseBase, String xmlPath, String xsdPath) throws Exception {
        // xml file to bean
        Shardings shardingBean;
        try {
            shardingBean = (Shardings) xmlParseBase.baseParseXmlToBean(xmlPath, xsdPath);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to Shardings is :" + shardingBean);
        }
        return shardingBeanToJson(shardingBean);
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

    private void schemaListToMap(List<Schema> schemaList, Map<String, ShardingNodeConfig> shardingNodeConfigMap, ProblemReporter problemReporter) {
        for (com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.schema.Schema schema : schemaList) {
            String schemaName = schema.getName();
            String schemaShardingNode = schema.getShardingNode();
            String schemaFunction = schema.getFunction();
            String schemaSqlMaxLimitStr = null == schema.getSqlMaxLimit() ? null : String.valueOf(schema.getSqlMaxLimit());
            List<Object> tableList = Optional.ofNullable(schema.getTable()).orElse(Lists.newArrayList());

            int schemaSqlMaxLimit = getSqlMaxLimit(schemaSqlMaxLimitStr, -1);
            List<String> shardingNodeList = null;
            AbstractPartitionAlgorithm algorithm = null;
            //check and add shardingNode
            if (schemaShardingNode != null && !schemaShardingNode.isEmpty()) {
                if (schemaFunction != null && !schemaFunction.isEmpty()) {
                    if (StringUtil.isBlank(schemaFunction)) {
                        throw new ConfigException("function of " + schemaFunction + " is empty");
                    }
                    algorithm = this.functionMap.get(schemaFunction);
                    if (algorithm == null) {
                        throw new ConfigException("can't find function of name :" + schemaFunction + " in schema " + schemaName);
                    }

                    if (StringUtil.isBlank(schemaShardingNode)) {
                        throw new ConfigException("shardingNode of " + schemaShardingNode + " is empty");
                    }
                    String[] theShardingNodes = SplitUtil.split(schemaShardingNode, ',', '$', '-');
                    final long distinctCount = Arrays.stream(theShardingNodes).distinct().count();
                    if (distinctCount != theShardingNodes.length) {
                        //detected repeat props;
                        throw new ConfigException("invalid schema config: " + schemaShardingNode + " for schema " + schemaName + " ,the nodes duplicated!");
                    }
                    shardingNodeList = Arrays.asList(theShardingNodes);
                    checkShardingNodeExists(shardingNodeList, shardingNodeConfigMap);
                    checkRuleSuitSchema(schema, schemaFunction, algorithm, shardingNodeList, problemReporter);
                } else {
                    shardingNodeList = new ArrayList<>(1);
                    shardingNodeList.add(schemaShardingNode);
                    checkShardingNodeExists(shardingNodeList, shardingNodeConfigMap);
                }
            }

            //load tables from sharding
            Map<String, BaseTableConfig> tableConfigMap = Maps.newLinkedHashMap();
            if (this.schemaConfigMap.containsKey(schemaName)) {
                throw new ConfigException("schema " + schemaName + " duplicated!");
            }

            for (Object tableObj : tableList) {
                if (tableObj instanceof ShardingTable) {
                    fillShardingTable((ShardingTable) tableObj, schemaSqlMaxLimit, tableConfigMap, shardingNodeConfigMap, problemReporter);
                } else if (tableObj instanceof GlobalTable) {
                    fillGlobalTable((GlobalTable) tableObj, schemaSqlMaxLimit, tableConfigMap, shardingNodeConfigMap);
                } else if (tableObj instanceof SingleTable) {
                    fillSingleTable((SingleTable) tableObj, schemaSqlMaxLimit, tableConfigMap, shardingNodeConfigMap);
                }
            }

            // if sharding has no default shardingNode,it must contains at least one table
            if (shardingNodeList == null && tableConfigMap.size() == 0) {
                throw new ConfigException(
                        "sharding " + schemaName + " didn't config tables,so you must set shardingNode property!");
            }
            SchemaConfig schemaConfig = new SchemaConfig(schemaName, shardingNodeList, algorithm, tableConfigMap, schemaSqlMaxLimit, schema.isLogicalCreateADrop());
            mergeFuncNodeERMap(schemaConfig);
            mergeFkERMap(schemaConfig);
            this.schemaConfigMap.put(schemaName, schemaConfig);
        }
        makeAllErRelations();
    }

    public static void removeFileContent(List<Function> functionList) {
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

                proList.removeAll(writeData);

                tempData.clear();
                writeData.clear();
            }
        }

    }

    private void fillSingleTable(SingleTable singleTable, int schemaSqlMaxLimit, Map<String, BaseTableConfig> tableConfigMap, Map<String, ShardingNodeConfig> shardingNodeConfigMap) {
        String singleTableName = singleTable.getName();
        String singleTableSqlMaxLimitStr = null == singleTable.getSqlMaxLimit() ? null : String.valueOf(singleTable.getSqlMaxLimit());
        String singleTableShardingNode = singleTable.getShardingNode();
        boolean specifyCharset = singleTable.getSpecifyCharset();

        if (StringUtil.isBlank(singleTableName)) {
            throw new ConfigException("one of tables' name is empty");
        }
        //limit size of the table
        int tableSqlMaxLimit = getSqlMaxLimit(singleTableSqlMaxLimitStr, schemaSqlMaxLimit);
        if (StringUtil.isBlank(singleTableShardingNode)) {
            throw new ConfigException("shardingNode of " + singleTableName + " is empty");
        }
        String[] theShardingNodes = SplitUtil.split(singleTableShardingNode, ',', '$', '-');
        if (theShardingNodes.length != 1) {
            throw new ConfigException("invalid shardingNode config: " + singleTableShardingNode + " for SingleTableConfig " + singleTableName);
        }
        String[] tableNames = singleTableName.split(",");

        for (String tableName : tableNames) {
            if (tableName.contains("`")) {
                tableName = tableName.replaceAll("`", "");
            }
            if (StringUtil.isBlank(tableName)) {
                throw new ConfigException("one of table name of " + singleTableName + " is empty");
            }
            SingleTableConfig table = new SingleTableConfig(tableName, tableSqlMaxLimit, Arrays.asList(theShardingNodes), specifyCharset);
            checkShardingNodeExists(table.getShardingNodes(), shardingNodeConfigMap);
            if (tableConfigMap.containsKey(table.getName())) {
                throw new ConfigException("table " + tableName + " duplicated!");
            }
            table.setId(this.tableIndex.incrementAndGet());
            tableConfigMap.put(table.getName(), table);
        }
    }

    private void fillGlobalTable(GlobalTable globalTable, int schemaSqlMaxLimit, Map<String, BaseTableConfig> tableConfigMap, Map<String, ShardingNodeConfig> shardingNodeConfigMap) {
        String globalTableName = globalTable.getName();
        String globalTableSqlMaxLimitStr = null == globalTable.getSqlMaxLimit() ? null : String.valueOf(globalTable.getSqlMaxLimit());
        String globalTableShardingNode = globalTable.getShardingNode();
        String globalTableCheckClass = Optional.ofNullable(globalTable.getCheckClass()).orElse(GLOBAL_TABLE_CHECK_DEFAULT);
        String globalTableCron = Optional.ofNullable(globalTable.getCron()).orElse(GLOBAL_TABLE_CHECK_DEFAULT_CRON).toUpperCase();
        boolean globalCheck = !StringUtil.isBlank(globalTable.getCheckClass());
        boolean specifyCharset = globalTable.getSpecifyCharset();

        if (StringUtil.isBlank(globalTableName)) {
            throw new ConfigException("one of tables' name is empty");
        }
        //limit size of the table
        int tableSqlMaxLimit = getSqlMaxLimit(globalTableSqlMaxLimitStr, schemaSqlMaxLimit);
        if (StringUtil.isBlank(globalTableShardingNode)) {
            throw new ConfigException("shardingNode of " + globalTableName + " is empty");
        }
        String[] theShardingNodes = SplitUtil.split(globalTableShardingNode, ',', '$', '-');
        final long distinctCount = Arrays.stream(theShardingNodes).distinct().count();
        if (distinctCount != theShardingNodes.length) {
            //detected repeat props;
            throw new ConfigException("invalid shardingNode config: " + globalTableShardingNode + " for GlobalTableConfig " + globalTableName + ",the nodes duplicated!");
        }
        if (theShardingNodes.length <= 1) {
            throw new ConfigException("invalid shardingNode config: " + globalTableShardingNode + " for GlobalTableConfig " + globalTableName + ", please use SingleTable");
        }
        String[] tableNames = globalTableName.split(",");

        for (String tableName : tableNames) {
            if (tableName.contains("`")) {
                tableName = tableName.replaceAll("`", "");
            }
            if (StringUtil.isBlank(tableName)) {
                throw new ConfigException("one of table name of " + globalTableName + " is empty");
            }
            GlobalTableConfig table = new GlobalTableConfig(tableName, tableSqlMaxLimit, Arrays.asList(theShardingNodes),
                    globalTableCron, globalTableCheckClass, globalCheck, specifyCharset);
            checkShardingNodeExists(table.getShardingNodes(), shardingNodeConfigMap);
            if (tableConfigMap.containsKey(table.getName())) {
                throw new ConfigException("table " + tableName + " duplicated!");
            }
            table.setId(this.tableIndex.incrementAndGet());
            tableConfigMap.put(table.getName(), table);
        }
    }

    private void fillShardingTable(ShardingTable shardingTable, int schemaSqlMaxLimit, Map<String, BaseTableConfig> tableConfigMap, Map<String, ShardingNodeConfig> shardingNodeConfigMap, ProblemReporter problemReporter) {
        String shardingTableName = shardingTable.getName();
        String shardingTableSqlMaxLimitStr = null == shardingTable.getSqlMaxLimit() ? null : String.valueOf(shardingTable.getSqlMaxLimit());
        String shardingTableShardingColumn = shardingTable.getShardingColumn();
        boolean shardingTableSqlRequiredSharding = Optional.ofNullable(shardingTable.getSqlRequiredSharding()).orElse(false);
        boolean specifyCharset = shardingTable.getSpecifyCharset();

        if (StringUtil.isBlank(shardingTableName)) {
            throw new ConfigException("one of tables' name is empty");
        }
        int tableSqlMaxLimit = getSqlMaxLimit(shardingTableSqlMaxLimitStr, schemaSqlMaxLimit);
        //shardingNode of table
        if (StringUtil.isBlank(shardingTableShardingColumn)) {
            throw new ConfigException("shardingColumn of " + shardingTableName + " is empty");
        }
        shardingTableShardingColumn = shardingTableShardingColumn.toUpperCase();
        String shardingTableIncrementColumn = StringUtil.isBlank(shardingTable.getIncrementColumn()) ? null : shardingTable.getIncrementColumn().toUpperCase();
        String shardingTableFunction = shardingTable.getFunction();
        if (StringUtil.isBlank(shardingTableFunction)) {
            throw new ConfigException("function of " + shardingTableName + " is empty");
        }
        AbstractPartitionAlgorithm algorithm = this.functionMap.get(shardingTableFunction);
        if (algorithm == null) {
            throw new ConfigException("can't find function of name :" + shardingTableFunction + " in table " + shardingTableName);
        }
        String shardingTableShardingNode = shardingTable.getShardingNode();
        if (StringUtil.isBlank(shardingTableShardingNode)) {
            throw new ConfigException("shardingNode of " + shardingTableName + " is empty");
        }
        String[] theShardingNodes = SplitUtil.split(shardingTableShardingNode, ',', '$', '-');
        final long distinctCount = Arrays.stream(theShardingNodes).distinct().count();
        if (distinctCount != theShardingNodes.length) {
            //detected repeat props;

            throw new ConfigException("invalid shardingNode config: " + shardingTableShardingNode + " for ShardingTableConfig " + shardingTableName + " ,the nodes duplicated!");
        }
        if (theShardingNodes.length <= 1) {
            throw new ConfigException("invalid shardingNode config: " + shardingTableShardingNode + " for ShardingTableConfig " + shardingTableName + ", please use SingleTable");
        }
        List<String> lstShardingNode = Arrays.asList(theShardingNodes);
        String[] tableNames = shardingTableName.split(",");

        for (String tableName : tableNames) {
            if (tableName.contains("`")) {
                tableName = tableName.replaceAll("`", "");
            }
            if (StringUtil.isBlank(tableName)) {
                throw new ConfigException("one of table name of " + shardingTableName + " is empty");
            }
            ShardingTableConfig table = new ShardingTableConfig(tableName, tableSqlMaxLimit,
                    lstShardingNode, shardingTableIncrementColumn, algorithm, shardingTableShardingColumn, shardingTableSqlRequiredSharding, specifyCharset);
            checkShardingNodeExists(table.getShardingNodes(), shardingNodeConfigMap);
            checkRuleSuitTable(table, shardingTableFunction, problemReporter);
            if (tableConfigMap.containsKey(table.getName())) {
                throw new ConfigException("table " + tableName + " duplicated!");
            }
            table.setId(this.tableIndex.incrementAndGet());
            tableConfigMap.put(table.getName(), table);
        }
        // child table must know its unique father
        if (tableNames.length == 1) {
            ShardingTableConfig parentTable = (ShardingTableConfig) (tableConfigMap.get(tableNames[0]));
            // process child tables
            final List<ChildTable> childTableList = shardingTable.getChildTable();
            processChildTables(tableConfigMap, parentTable, lstShardingNode, childTableList, schemaSqlMaxLimit);
        }
    }

    private void makeAllErRelations() {
        Iterator<Map.Entry<String, Set<ERTable>>> iterator = this.funcNodeERMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<ERTable>> entry = iterator.next();
            if (entry.getValue().size() == 1) {
                continue;
            }
            for (ERTable erTable : entry.getValue()) {
                Set<ERTable> relations = this.erRelations.get(erTable);
                if (relations == null) {
                    this.erRelations.put(erTable, entry.getValue());
                } else {
                    relations.addAll(entry.getValue());
                }
            }
        }
    }


    private void deleteUselessShardingNode(List<ErrorInfo> errorInfos, RawJson sequenceJson) {
        Set<String> allUseShardingNode = new HashSet<>();
        for (SchemaConfig sc : this.schemaConfigMap.values()) {
            // check shardingNode / dbGroup
            Set<String> shardingNodeNames = sc.getAllShardingNodes();
            allUseShardingNode.addAll(shardingNodeNames);
        }

        // add global sequence node when it is some dedicated servers */
        allUseShardingNode.addAll(SequenceManager.getShardingNodes(sequenceJson));

        //delete redundancy shardingNode
        Iterator<Map.Entry<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode>> iterator = this.shardingNodeMap.entrySet().iterator();
        PhysicalDbGroup shardingNodeGroup;
        while (iterator.hasNext()) {
            Map.Entry<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode> entry = iterator.next();
            String shardingNodeName = entry.getKey();
            if (allUseShardingNode.contains(shardingNodeName)) {
                shardingNodeGroup = entry.getValue().getDbGroup();
                if (shardingNodeGroup != null) {
                    shardingNodeGroup.setShardingUseless(false);
                } else {
                    throw new ConfigException("The dbGroup[" + entry.getValue().getDbGroupName() + "] associated with ShardingNode[" + entry.getKey() + "] does not exist");
                }
                if (shardingNodeGroup.getDbGroupConfig().getWriteInstanceConfig().getDataBaseType() != DataBaseType.MYSQL) {
                    throw new ConfigException("The dbGroup[" + entry.getValue().getDbGroupName() + "] not support database type [" + entry.getKey() + "]");
                }
            } else {
                errorInfos.add(new ErrorInfo("Xml", "WARNING", "shardingNode " + shardingNodeName + " is useless"));
                iterator.remove();
            }
        }
    }

    private void mergeFkERMap(SchemaConfig schemaConfig) {
        Map<ERTable, Set<ERTable>> schemaFkERMap = schemaConfig.getFkErRelations();
        if (schemaFkERMap == null) {
            return;
        }
        this.erRelations.putAll(schemaFkERMap);
    }

    private void mergeFuncNodeERMap(SchemaConfig schemaConfig) {
        Map<String, Set<ERTable>> schemaFuncNodeER = schemaConfig.getFuncNodeERMap();
        if (schemaFuncNodeER == null) {
            return;
        }
        for (Map.Entry<String, Set<ERTable>> entry : schemaFuncNodeER.entrySet()) {
            String key = entry.getKey();
            if (!this.funcNodeERMap.containsKey(key)) {
                this.funcNodeERMap.put(key, entry.getValue());
            } else {
                Set<ERTable> setFuncNode = this.funcNodeERMap.get(key);
                setFuncNode.addAll(entry.getValue());
            }
        }
    }

    private void processChildTables(Map<String, BaseTableConfig> tables, BaseTableConfig parentTable, List<String> lstShardingNode, List<ChildTable> shardingTable,
                                    int schemaSqlMaxLimit) {
        for (ChildTable childTable : shardingTable) {
            String childTableName = childTable.getName();
            String childTableSqlMaxLimitStr = null == childTable.getSqlMaxLimit() ? null : String.valueOf(childTable.getSqlMaxLimit());
            String childTableJoinColumn = childTable.getJoinColumn().toUpperCase();
            String childTableParentColumn = childTable.getParentColumn().toUpperCase();
            String childTableIncrementColumn = StringUtil.isBlank(childTable.getIncrementColumn()) ? null : childTable.getIncrementColumn().toUpperCase();

            if (StringUtil.isBlank(childTableName)) {
                throw new ConfigException("one of table [" + parentTable.getName() + "]'s child name is empty");
            }
            int tableSqlMaxLimit = getSqlMaxLimit(childTableSqlMaxLimitStr, schemaSqlMaxLimit);

            ChildTableConfig table = new ChildTableConfig(childTableName, tableSqlMaxLimit, lstShardingNode,
                    parentTable, childTableJoinColumn, childTableParentColumn, childTableIncrementColumn, childTable.isSpecifyCharset());

            if (tables.containsKey(table.getName())) {
                throw new ConfigException("table " + table.getName() + " duplicated!");
            }
            table.setId(this.tableIndex.incrementAndGet());
            tables.put(table.getName(), table);
            //child table may also have children
            List<ChildTable> childTableList = childTable.getChildTable();
            processChildTables(tables, table, lstShardingNode, childTableList, schemaSqlMaxLimit);
        }

    }

    private void checkShardingNodeExists(List<String> nodes, Map<String, ShardingNodeConfig> shardingNodeConfigMap) {
        if (nodes == null || nodes.size() < 1) {
            return;
        }
        for (String node : nodes) {
            if (!shardingNodeConfigMap.containsKey(node)) {
                throw new ConfigException("shardingNode '" + node + "' is not found!");
            }
        }
    }

    private void checkRuleSuitTable(ShardingTableConfig tableConf, String functionName, ProblemReporter problemReporter) {
        AbstractPartitionAlgorithm function = tableConf.getFunction();
        int suitValue = function.suitableFor(tableConf.getShardingNodes().size());
        if (suitValue < 0) {
            throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ " +
                    functionName + " ] partition size : " + tableConf.getFunction().getPartitionNum() + " > table shardingNode size : " +
                    tableConf.getShardingNodes().size() + ", please make sure table shardingnode size = function partition size");
        } else if (suitValue > 0) {
            problemReporter.warn("table conf : table [ " + tableConf.getName() + " ] rule function [ " + functionName + " ] " +
                    "partition size : " + tableConf.getFunction().getPartitionNum() + " < table shardingNode size : " + tableConf.getShardingNodes().size());
        }
    }

    private void checkRuleSuitSchema(Schema schema, String functionName, AbstractPartitionAlgorithm function, List<String> shardingNodeLst, ProblemReporter problemReporter) {
        int suitValue = function.suitableFor(shardingNodeLst.size());
        if (suitValue < 0) {
            throw new ConfigException("Illegal schema conf : schema [ " + schema.getName() + " ] rule function [ " +
                    functionName + " ] partition size : " + function.getPartitionNum() + " > schema shardingNode size : " +
                    shardingNodeLst.size() + ", please make sure schema shardingnode size = function partition size");
        } else if (suitValue > 0) {
            problemReporter.warn("schema conf : schema [ " + schema.getName() + " ] rule function [ " + functionName + " ] " +
                    "partition size : " + function.getPartitionNum() + " < schema shardingNode size : " + shardingNodeLst.size());
        }
    }

    private void functionListToMap(List<Function> functionList, ProblemReporter problemReporter) throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException {
        for (Function function : functionList) {
            String functionName = function.getName();
            String functionClazz = function.getClazz();
            List<Property> propertyList = function.getProperty();

            // check if the function is duplicate
            if (this.functionMap.containsKey(functionName)) {
                throw new ConfigException("rule function " + functionName + " duplicated!");
            }
            //reflection
            AbstractPartitionAlgorithm functionInstance = createFunction(functionName, functionClazz);
            functionInstance.setName(functionName);
            Properties props = new Properties();
            propertyList.forEach(property -> props.setProperty(property.getName(), property.getValue()));
            ParameterMapping.mapping(functionInstance, props, problemReporter);
            if (props.size() > 0) {
                String[] propItem = new String[props.size()];
                props.keySet().toArray(propItem);
                throw new ConfigException("These properties of function [" + functionName + "] is not recognized: " + StringUtil.join(propItem, ","));
            }

            ParameterMapping.checkMappingResult();
            //init for AbstractPartitionAlgorithm
            functionInstance.selfCheck();
            functionInstance.init();
            this.functionMap.put(functionName, functionInstance);
        }
        setFunctionAlias();
    }

    private void setFunctionAlias() {
        Map<AbstractPartitionAlgorithm, String> funcAlias = new HashMap<>();
        int i = 0;
        for (AbstractPartitionAlgorithm function : this.functionMap.values()) {
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
            IllegalAccessException {

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

    private void shardingNodeListToMap(List<ShardingNode> shardingNodeList, Map<String, PhysicalDbGroup> dbGroupMap, Map<String, ShardingNodeConfig> shardingNodeConfigMap) {
        Set<String> checkSet = new HashSet<>();
        for (ShardingNode shardingNode : shardingNodeList) {
            String shardingNodeName = shardingNode.getName();
            String shardingNodeDatabase = shardingNode.getDatabase();
            String shardingNodeDbGroup = shardingNode.getDbGroup();
            PhysicalDbGroup dbGroup = dbGroupMap.get(shardingNodeDbGroup);
            if (StringUtils.isBlank(shardingNodeName) || StringUtils.isBlank(shardingNodeDatabase) || StringUtils.isBlank(shardingNodeDbGroup)) {
                throw new ConfigException("shardingNode " + shardingNodeName + " define error ,attribute can't be empty");
            }
            if (Objects.nonNull(dbGroup) && dbGroup.getDbGroupConfig().instanceDatabaseType() != DataBaseType.MYSQL) {
                throw new ConfigException("shardingNodeDbGroup [" + shardingNodeDbGroup + "] define error ,all dbInstance database type must be " + DataBaseType.MYSQL);
            }
            //dnNamePre(name),databaseStr(database),host(dbGroup) can use ',', '$', '-' to configure multi nodes
            // but the database size *dbGroup size must equal the size of name
            // every dbGroup has all database in its tag
            //eg:<shardingNode name="dn$0-759" dbGroup="localhost$1-10" database="db$0-75" />
            //means:localhost1 has database of db$0-75,localhost2 has database of db$0-75(name is dn$76-151)
            String[] dnNames = SplitUtil.split(shardingNodeName, ',', '$', '-');
            String[] databases = SplitUtil.split(shardingNodeDatabase, ',', '$', '-');
            String[] hostStrings = SplitUtil.split(shardingNodeDbGroup, ',', '$', '-');

            if (dnNames.length != databases.length * hostStrings.length) {
                throw new ConfigException("shardingNode " + shardingNodeName +
                        " define error ,Number of shardingNode name must be = Number of database * Number of dbGroup");
            }
            if (dnNames.length > 1) {
                List<String[]> mhdList = mergerHostDatabase(hostStrings, databases);
                for (int k = 0; k < dnNames.length; k++) {
                    String[] hd = mhdList.get(k);
                    String dnName = dnNames[k];
                    String databaseName = hd[1];
                    String hostName = hd[0];
                    createShardingNode(dnName, databaseName, hostName, checkSet, shardingNodeConfigMap);
                }
            } else {
                createShardingNode(shardingNodeName, shardingNodeDatabase, shardingNodeDbGroup, checkSet, shardingNodeConfigMap);
            }
        }
        this.shardingNodeMap = initShardingNodes(shardingNodeConfigMap, dbGroupMap);
    }

    private void createShardingNode(String dnName, String database, String host, Set<String> checkSet, Map<String, ShardingNodeConfig> shardingNodeConfigMap) {

        ShardingNodeConfig conf = new ShardingNodeConfig(dnName, database, host);
        if (checkSet.contains(host + "#" + database)) {
            throw new ConfigException("shardingNode " + conf.getName() + " use the same dbGroup&database with other shardingNode");
        } else {
            checkSet.add(host + "#" + database);
        }
        if (shardingNodeConfigMap.containsKey(conf.getName())) {
            throw new ConfigException("shardingNode " + conf.getName() + " duplicated!");
        }
        shardingNodeConfigMap.put(conf.getName(), conf);
    }

    private Map<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode> initShardingNodes(Map<String, ShardingNodeConfig> nodeConf, Map<String, PhysicalDbGroup> dbGroupMap) {
        Map<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode> nodes = new HashMap<>(nodeConf.size());
        for (ShardingNodeConfig conf : nodeConf.values()) {
            PhysicalDbGroup pool = dbGroupMap.get(conf.getDbGroupName());
            com.oceanbase.obsharding_d.backend.datasource.ShardingNode shardingNode = new com.oceanbase.obsharding_d.backend.datasource.ShardingNode(conf.getDbGroupName(), conf.getName(), conf.getDatabase(), pool);
            nodes.put(shardingNode.getName(), shardingNode);
        }
        return nodes;
    }


    public Map<String, com.oceanbase.obsharding_d.backend.datasource.ShardingNode> getShardingNodeMap() {
        return shardingNodeMap;
    }

    public Map<String, AbstractPartitionAlgorithm> getFunctionMap() {
        return functionMap;
    }

    public Map<String, SchemaConfig> getSchemaConfigMap() {
        return schemaConfigMap;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    public Map<String, Set<ERTable>> getFuncNodeERMap() {
        return funcNodeERMap;
    }

    private static int getSqlMaxLimit(String sqlMaxLimitStr, int defaultMaxLimit) {
        // sql result size limit
        if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
            defaultMaxLimit = Integer.parseInt(sqlMaxLimitStr);
            if (defaultMaxLimit < -1) {
                defaultMaxLimit = -1;
            }
        }
        return defaultMaxLimit;
    }

    private static List<String[]> mergerHostDatabase(String[] hostStrings, String[] databases) {
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
}
