/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.config.loader.xml;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.model.*;
import io.mycat.config.model.TableConfig.TableTypeEnum;
import io.mycat.config.model.rule.TableRuleConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.config.util.ConfigUtil;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.util.DecryptUtil;
import io.mycat.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
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
        //先读取rule.xml
        XMLRuleLoader ruleLoader = new XMLRuleLoader(ruleFile);
        //将tableRules拿出，用于这里加载Schema做rule有效判断，以及之后的分片路由计算
        this.tableRules = ruleLoader.getTableRules();
        //释放ruleLoader
        ruleLoader = null;
        this.dataHosts = new HashMap<>();
        this.dataNodes = new HashMap<>();
        this.schemas = new HashMap<>();
        this.lowerCaseNames = lowerCaseNames;
        //读取加载schema配置
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
            //先加载所有的DataHost
            loadDataHosts(root);
            //再加载所有的DataNode
            loadDataNodes(root);
            //最后加载所有的Schema
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
            //读取各个属性
            String name = schemaElement.getAttribute("name");
            if (lowerCaseNames) {
                name = name.toLowerCase();
            }
            String dataNode = schemaElement.getAttribute("dataNode");
            String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
            int sqlMaxLimit = -1;
            //读取sql返回结果集限制
            if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
                sqlMaxLimit = Integer.parseInt(sqlMaxLimitStr);
            }
            //校验检查并添加dataNode
            if (dataNode != null && !dataNode.isEmpty()) {
                List<String> dataNodeLst = new ArrayList<>(1);
                dataNodeLst.add(dataNode);
                checkDataNodeExists(dataNodeLst);
            } else {
                dataNode = null;
            }
            //加载schema下所有tables
            Map<String, TableConfig> tables = loadTables(schemaElement, lowerCaseNames);
            //判断schema是否重复
            if (schemas.containsKey(name)) {
                throw new ConfigException("schema " + name + " duplicated!");
            }

            // 设置了table的不需要设置dataNode属性，没有设置table的必须设置dataNode属性
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
        schemaFkERMap = null;
    }

    /**
     * 处理动态日期表, 支持 YYYYMM、YYYYMMDD 两种格式
     * <p>
     * YYYYMM格式：       yyyymm,2015,01,60
     * YYYYMMDD格式:  yyyymmdd,2015,01,10,50
     *
     * @param tableNameElement
     * @param tableNameSuffixElement
     * @return
     */
    private String doTableNameSuffix(String tableNameElement, String tableNameSuffixElement) {

        String newTableName = tableNameElement;

        String[] params = tableNameSuffixElement.split(",");
        String suffixFormat = params[0];
        if (suffixFormat.equals("YYYYMM")) {

            //日期处理
            SimpleDateFormat yyyyMMDateFormat = new SimpleDateFormat("yyyyMM");

            int yyyy = Integer.parseInt(params[1]);
            int mm = Integer.parseInt(params[2]);

            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.YEAR, yyyy);
            cal.set(Calendar.MONTH, mm - 1);
            cal.set(Calendar.DATE, 0);

            //表名改写
            StringBuilder tableNameBuffer = new StringBuilder();
            int mmEndIdx = Integer.parseInt(params[3]);
            for (int mmIdx = 0; mmIdx <= mmEndIdx; mmIdx++) {
                tableNameBuffer.append(tableNameElement);
                tableNameBuffer.append(yyyyMMDateFormat.format(cal.getTime()));
                cal.add(Calendar.MONTH, 1);

                if (mmIdx != mmEndIdx) {
                    tableNameBuffer.append(",");
                }
            }
            newTableName = tableNameBuffer.toString();

        } else if (suffixFormat.equals("YYYYMMDD")) {

            //日期处理
            SimpleDateFormat yyyyMMddSDF = new SimpleDateFormat("yyyyMMdd");

            int yyyy = Integer.parseInt(params[1]);
            int mm = Integer.parseInt(params[2]);
            int dd = Integer.parseInt(params[3]);

            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.YEAR, yyyy);
            cal.set(Calendar.MONTH, mm - 1);
            cal.set(Calendar.DATE, dd);

            //表名改写
            int ddEndIdx = Integer.parseInt(params[4]);
            StringBuilder tableNameBuffer = new StringBuilder();
            for (int ddIdx = 0; ddIdx <= ddEndIdx; ddIdx++) {
                tableNameBuffer.append(tableNameElement);
                tableNameBuffer.append(yyyyMMddSDF.format(cal.getTime()));

                cal.add(Calendar.DATE, 1);

                if (ddIdx != ddEndIdx) {
                    tableNameBuffer.append(",");
                }
            }
            newTableName = tableNameBuffer.toString();
        }
        return newTableName;
    }


    private Map<String, TableConfig> loadTables(Element node, boolean isLowerCaseNames) {
        // 支持表名中包含引号[`] BEN GONG
        Map<String, TableConfig> tables = new TableConfigMap();
        NodeList nodeList = node.getElementsByTagName("table");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String tableNameElement = tableElement.getAttribute("name");
            if (isLowerCaseNames) {
                tableNameElement = tableNameElement.toLowerCase();
            }

            //TODO:路由, 增加对动态日期表的支持
            String tableNameSuffixElement = tableElement.getAttribute("nameSuffix");
            if (isLowerCaseNames) {
                tableNameSuffixElement = tableNameSuffixElement.toLowerCase();
            }
            if (!"".equals(tableNameSuffixElement)) {

                if (tableNameElement.split(",").length > 1) {
                    throw new ConfigException("nameSuffix " + tableNameSuffixElement + ", require name parameter cannot multiple breaks!");
                }
                //前缀用来标明日期格式
                tableNameElement = doTableNameSuffix(tableNameElement, tableNameSuffixElement);
            }
            //记录主键，用于之后路由分析，以及启用自增长主键
            String primaryKey = tableElement.hasAttribute("primaryKey") ? tableElement.getAttribute("primaryKey").toUpperCase() : null;
            //记录是否主键自增，默认不是，（启用全局sequence handler）
            boolean autoIncrement = false;
            if (tableElement.hasAttribute("autoIncrement")) {
                autoIncrement = Boolean.parseBoolean(tableElement.getAttribute("autoIncrement"));
            }
            //记录是否需要加返回结果集限制，默认需要加
            boolean needAddLimit = true;
            if (tableElement.hasAttribute("needAddLimit")) {
                needAddLimit = Boolean.parseBoolean(tableElement.getAttribute("needAddLimit"));
            }
            //记录type，是否为global
            String tableTypeStr = tableElement.hasAttribute("type") ? tableElement.getAttribute("type") : null;
            TableTypeEnum tableType = TableTypeEnum.TYPE_SHARDING_TABLE;
            if ("global".equalsIgnoreCase(tableTypeStr)) {
                tableType = TableTypeEnum.TYPE_GLOBAL_TABLE;
            }
            //记录dataNode，就是分布在哪些dataNode上
            TableRuleConfig tableRule = null;
            if (tableElement.hasAttribute("rule")) {
                String ruleName = tableElement.getAttribute("rule");
                tableRule = tableRules.get(ruleName);
                if (tableRule == null) {
                    throw new ConfigException("rule " + ruleName + " is not found!");
                }
            }
            boolean ruleRequired = false;
            //记录是否绑定有分片规则
            if (tableElement.hasAttribute("ruleRequired")) {
                ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
            }

            String[] tableNames = tableNameElement.split(",");
            if (tableNames == null) {
                throw new ConfigException("table name is not found!");
            }
            //distribute函数，重新编排dataNode
            String distPrex = "distribute(";
            String dataNode = tableElement.getAttribute("dataNode");
            boolean distTableDns = dataNode.startsWith(distPrex);
            if (distTableDns) {
                dataNode = dataNode.substring(distPrex.length(), dataNode.length() - 1);
            }

            for (String tableName : tableNames) {
                TableConfig table = new TableConfig(tableName, primaryKey, autoIncrement, needAddLimit, tableType,
                        dataNode, (tableRule != null) ? tableRule.getRule() : null, ruleRequired);
                checkDataNodeExists(table.getDataNodes());
                // 检查分片表分片规则配置是否合法
                if (table.getRule() != null) {
                    checkRuleSuitTable(table);
                }
                if (distTableDns) {
                    distributeDataNodes(table.getDataNodes());
                }
                //检查去重
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                //放入map
                tables.put(table.getName(), table);
            }
            //只有tableName配置的是单个表（没有逗号）的时候才能有子表
            if (tableNames.length == 1) {
                TableConfig table = tables.get(tableNames[0]);
                // process child tables
                processChildTables(tables, table, dataNode, tableElement, isLowerCaseNames);
            }
        }
        return tables;
    }

    /**
     * distribute datanodes in multi hosts,means ,dn1 (host1),dn100
     * (host2),dn300(host3),dn2(host1),dn101(host2),dn301(host3)...etc
     * 将每个host上的datanode按照host重新排列。比如上面的例子host1拥有dn1,dn2，host2拥有dn100，dn101，host3拥有dn300，dn301,
     * 按照host重新排列： 0->dn1 (host1),1->dn100(host2),2->dn300(host3),3->dn2(host1),4->dn101(host2),5->dn301(host3)
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
            //读取子表信息
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
            //子表join键，和对应的parent的键，父子表通过这个关联
            String joinKey = childTbElement.getAttribute("joinKey").toUpperCase();
            String parentKey = childTbElement.getAttribute("parentKey").toUpperCase();
            TableConfig table = new TableConfig(cdTbName, primaryKey, autoIncrement, needAddLimit,
                    TableTypeEnum.TYPE_SHARDING_TABLE, strDatoNodes, null, false, parentTable, joinKey, parentKey);

            if (tables.containsKey(table.getName())) {
                throw new ConfigException("table " + table.getName() + " duplicated!");
            }
            tables.put(table.getName(), table);
            //对于子表的子表，递归处理
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
     * 检查分片表分片规则配置, 目前主要检查分片表分片算法定义与分片dataNode是否匹配<br>
     * 例如分片表定义如下:<br>
     * {@code
     * <table name="hotnews" primaryKey="ID" autoIncrement="true" dataNode="dn1,dn2"
     * rule="mod-long" />
     * }
     * <br>
     * 分片算法如下:<br>
     * {@code
     * <function name="mod-long" class="io.mycat.route.function.PartitionByMod">
     * <!-- how many data nodes -->
     * <property name="count">3</property>
     * </function>
     * }
     * <br>
     * shard table datanode(2) < function count(3) 此时检测为不匹配
     */
    private void checkRuleSuitTable(TableConfig tableConf) {
        AbstractPartitionAlgorithm function = tableConf.getRule().getRuleAlgorithm();
        int suitValue = function.suitableFor(tableConf);
        if (suitValue < 0) { // 少节点,给提示并抛异常
            throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ " +
                    tableConf.getRule().getFunctionName() + " ] partition size : " + tableConf.getRule().getRuleAlgorithm().getPartitionNum() + " > table datanode size : " +
                    tableConf.getDataNodes().size() + ", please make sure table datanode size = function partition size");
        } else if (suitValue > 0) { // 有些节点是多余的,给出warn log
            LOGGER.warn("table conf : table [ {} ] rule function [ {} ] partition size : {} < table datanode size : {} , this cause some datanode to be redundant",
                    new String[]{
                            tableConf.getName(),
                            tableConf.getRule().getFunctionName(),
                            String.valueOf(tableConf.getRule().getRuleAlgorithm().getPartitionNum()),
                            String.valueOf(tableConf.getDataNodes().size()),
                    });

        } else {
            // table datanode size == rule function partition size
        }
    }

    private void loadDataNodes(Element root) {
        //读取DataNode分支
        NodeList list = root.getElementsByTagName("dataNode");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element element = (Element) list.item(i);
            String dnNamePre = element.getAttribute("name");

            String databaseStr = element.getAttribute("database");
            if (lowerCaseNames) {
                databaseStr = databaseStr.toLowerCase();
            }
            String host = element.getAttribute("dataHost");
            //字符串不为空
            if (empty(dnNamePre) || empty(databaseStr) || empty(host)) {
                throw new ConfigException("dataNode " + dnNamePre + " define error ,attribute can't be empty");
            }
            //dnNames（name）,databases（database）,hostStrings（dataHost）都可以配置多个，以',', '$', '-'区分，但是需要保证database的个数*dataHost的个数=name的个数
            //多个dataHost与多个database如果写在一个标签，则每个dataHost拥有所有database
            //例如：<dataNode name="dn1$0-75" dataHost="localhost$1-10" database="db$0-759" />
            //则为：localhost1拥有dn1$0-75,localhost2也拥有dn1$0-75（对应db$76-151）
            String[] dnNames = io.mycat.util.SplitUtil.split(dnNamePre, ',', '$', '-');
            String[] databases = io.mycat.util.SplitUtil.split(databaseStr, ',', '$', '-');
            String[] hostStrings = io.mycat.util.SplitUtil.split(host, ',', '$', '-');

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
     * 匹配DataHost和Database，每个DataHost拥有每个Database名字
     *
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
        conf.setWeight(weight);    //新增权重
        return conf;
    }

    private void loadDataHosts(Element root) {
        NodeList list = root.getElementsByTagName("dataHost");
        for (int i = 0, n = list.getLength(); i < n; ++i) {

            Element element = (Element) list.item(i);
            String name = element.getAttribute("name");
            //判断是否重复
            if (dataHosts.containsKey(name)) {
                throw new ConfigException("dataHost name " + name + "duplicated!");
            }
            //读取最大连接数
            int maxCon = Integer.parseInt(element.getAttribute("maxCon"));
            //读取最小连接数
            int minCon = Integer.parseInt(element.getAttribute("minCon"));
            /**
             * 读取负载均衡配置
             * 1. balance="0", 不开启分离机制，所有读操作都发送到当前可用的 writeHost 上。
             * 2. balance="1"，全部的 readHost 和 stand by writeHost 参不 select 的负载均衡
             * 3. balance="2"，所有读操作都随机的在 writeHost、readhost 上分发。
             * 4. balance="3"，所有读请求随机的分发到 wiriterHost 对应的 readhost 执行，writerHost 不负担读压力
             */
            final int balance = Integer.parseInt(element.getAttribute("balance"));
            /**
             * 读取切换类型
             * -1 表示不自动切换
             * 1 默认值，自动切换
             * 2 基于MySQL主从同步的状态决定是否切换
             * 心跳询句为 show slave status
             * 3 基于 MySQL galary cluster 的切换机制
             */
            String switchTypeStr = element.getAttribute("switchType");
            int switchType = switchTypeStr.equals("") ? -1 : Integer.parseInt(switchTypeStr);
            //读取从延迟界限
            String slaveThresholdStr = element.getAttribute("slaveThreshold");
            int slaveThreshold = slaveThresholdStr.equals("") ? -1 : Integer.parseInt(slaveThresholdStr);

            //如果 tempReadHostAvailable 设置大于 0 则表示写主机如果挂掉， 临时的读服务依然可用
            String tempReadHostAvailableStr = element.getAttribute("tempReadHostAvailable");
            boolean tempReadHostAvailable = !tempReadHostAvailableStr.equals("") && Integer.parseInt(tempReadHostAvailableStr) > 0;

            //读取心跳语句
            final String heartbeatSQL = element.getElementsByTagName("heartbeat").item(0).getTextContent();

            //读取writeHost
            NodeList writeNodes = element.getElementsByTagName("writeHost");
            DBHostConfig[] writeDbConfs = new DBHostConfig[writeNodes.getLength()];
            Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<>(2);
            for (int w = 0; w < writeDbConfs.length; w++) {
                Element writeNode = (Element) writeNodes.item(w);
                writeDbConfs[w] = createDBHostConf(name, writeNode, maxCon, minCon);
                NodeList readNodes = writeNode.getElementsByTagName("readHost");
                //读取对应的每一个readHost
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
