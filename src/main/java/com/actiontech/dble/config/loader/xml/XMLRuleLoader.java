/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.model.rule.RuleConfig;
import com.actiontech.dble.config.model.rule.TableRuleConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.route.function.*;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLSyntaxErrorException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */
@SuppressWarnings("unchecked")
public class XMLRuleLoader {
    private static final String DEFAULT_DTD = "/rule.dtd";
    private static final String DEFAULT_XML = "/rule.xml";

    private final Map<String, TableRuleConfig> tableRules;
    private final Map<String, AbstractPartitionAlgorithm> functions;

    public XMLRuleLoader(String ruleFile) {
        this.tableRules = new HashMap<>();
        //function-> algorithm
        this.functions = new HashMap<>();
        load(DEFAULT_DTD, ruleFile == null ? DEFAULT_XML : ruleFile);
    }

    public Map<String, TableRuleConfig> getTableRules() {
        return (Map<String, TableRuleConfig>) (tableRules.isEmpty() ? Collections.emptyMap() : tableRules);
    }


    private void load(String dtdFile, String xmlFile) {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream(dtdFile);
            xml = ResourceUtil.getResourceAsStream(xmlFile);
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadFunctions(root);
            loadTableRules(root);
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

    /**
     * tableRule tag:
     * <tableRule name="sharding-by-month">
     * <rule>
     * <columns>create_date</columns>
     * <algorithm>partbymonth</algorithm>
     * </rule>
     * </tableRule>
     *
     * @param root
     * @throws SQLSyntaxErrorException
     */
    private void loadTableRules(Element root) throws SQLSyntaxErrorException {
        NodeList list = root.getElementsByTagName("tableRule");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                if (StringUtil.isEmpty(name)) {
                    throw new ConfigException("name is null or empty");
                }
                if (tableRules.containsKey(name)) {
                    throw new ConfigException("table rule " + name + " duplicated!");
                }
                NodeList ruleNodes = e.getElementsByTagName("rule");
                int length = ruleNodes.getLength();
                if (length > 1) {
                    throw new ConfigException("only one rule can defined :" + name);
                }
                //rule has only one element now. Maybe it will not contains one rule in feature
                //RuleConfig:rule->function
                RuleConfig rule = loadRule((Element) ruleNodes.item(0));
                tableRules.put(name, new TableRuleConfig(name, rule));
            }
        }
    }

    private RuleConfig loadRule(Element element) throws SQLSyntaxErrorException {
        Element columnsEle = ConfigUtil.loadElement(element, "columns");
        String column = columnsEle.getTextContent();
        if (StringUtil.isEmpty(column)) {
            throw new ConfigException("no rule column is found");
        }
        String[] columns = SplitUtil.split(column, ',', true);
        if (columns.length > 1) {
            throw new ConfigException("table rule coulmns has multi values:" +
                    columnsEle.getTextContent());
        }
        Element algorithmEle = ConfigUtil.loadElement(element, "algorithm");
        String algorithmName = algorithmEle.getTextContent();

        if (StringUtil.isEmpty(algorithmName)) {
            throw new ConfigException("algorithm is null or empty");
        }
        AbstractPartitionAlgorithm algorithm = functions.get(algorithmName);
        if (algorithm == null) {
            throw new ConfigException("can't find function of name :" + algorithmName);
        }
        return new RuleConfig(column.toUpperCase(), algorithmName, algorithm);
    }

    /**
     *
     * @param root
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
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
                ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
                //init for AbstractPartitionAlgorithm
                function.init();
                functions.put(name, function);
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
                    throw new IllegalArgumentException("rule function must implements " +
                            AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
                }
                return (AbstractPartitionAlgorithm) clz.newInstance();
        }

    }

}
