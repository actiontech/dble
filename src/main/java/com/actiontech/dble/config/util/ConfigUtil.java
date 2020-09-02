/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.TraceManager;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.*;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class ConfigUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtil.class);

    private ConfigUtil() {
    }

    public static String filter(String text) {
        return filter(text, System.getProperties());
    }

    public static String filter(String text, Properties properties) {
        StringBuilder s = new StringBuilder();
        int cur = 0;
        int textLen = text.length();
        int propStart;
        int propStop;
        String propName;
        String propValue;
        for (; cur < textLen; cur = propStop + 1) {
            propStart = text.indexOf("${", cur);
            if (propStart < 0) {
                break;
            }
            s.append(text.substring(cur, propStart));
            propStop = text.indexOf("}", propStart);
            if (propStop < 0) {
                throw new ConfigException("Unterminated property: " + text.substring(propStart));
            }
            propName = text.substring(propStart + 2, propStop);
            propValue = properties.getProperty(propName);
            if (propValue == null) {
                s.append("${").append(propName).append('}');
            } else {
                s.append(propValue);
            }
        }
        return s.append(text.substring(cur)).toString();
    }

    public static Document getDocument(final InputStream dtd, InputStream xml) throws ParserConfigurationException,
            SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, Boolean.TRUE);
        factory.setValidating(true);
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setEntityResolver(new EntityResolver() {
            @Override
            public InputSource resolveEntity(String publicId, String systemId) {
                return new InputSource(dtd);
            }
        });
        builder.setErrorHandler(new ErrorHandler() {
            @Override
            public void warning(SAXParseException e) {
            }

            @Override
            public void error(SAXParseException e) throws SAXException {
                throw e;
            }

            @Override
            public void fatalError(SAXParseException e) throws SAXException {
                throw e;
            }
        });
        return builder.parse(xml);
    }

    /**
     * @param parent parent
     * @return key-value property
     */
    public static Properties loadElements(Element parent) {
        Properties map = new Properties();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getNodeName();
                if ("property".equals(name)) {
                    String key = e.getAttribute("name");
                    String value = e.getTextContent();
                    if (value == null) {
                        map.put(key, "");
                    } else {
                        map.put(key, value.trim());
                    }
                }
            }
        }
        return map;
    }

    public static void setSchemasForPool(Map<String, PhysicalDbGroup> dbGroupMap, Map<String, ShardingNode> shardingNodeMap) {
        for (PhysicalDbGroup dbGroup : dbGroupMap.values()) {
            dbGroup.setSchemas(getShardingNodeSchemasOfDbGroup(dbGroup.getGroupName(), shardingNodeMap));
        }
    }

    private static String[] getShardingNodeSchemasOfDbGroup(String dbGroup, Map<String, ShardingNode> shardingNodeMap) {
        ArrayList<String> schemaList = new ArrayList<>(30);
        for (ShardingNode dn : shardingNodeMap.values()) {
            if (dn.getDbGroup() != null && dn.getDbGroup().getGroupName().equals(dbGroup)) {
                schemaList.add(dn.getDatabase());
            }
        }
        return schemaList.toArray(new String[schemaList.size()]);
    }

    private static boolean isNumeric(String value) {
        return "-1".equals(value) || StringUtils.isNumeric(value);
    }

    private static boolean isBool(String value) {
        return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
    }

    /**
     * check element illegal value and return val
     */
    public static String checkAndGetAttribute(Element element, String attrName, String defaultValue, ProblemReporter reporter) {
        if (element.hasAttribute(attrName)) {
            String val = element.getAttribute(attrName);
            if (isBool(val) || isNumeric(val)) {
                return val;
            } else if (reporter != null) {
                reporter.warn(element.getNodeName() + "[" + element.getAttribute("name") + "] attribute " + attrName + " " + val +
                        " is illegal, use " + defaultValue + " replaced");
            }
        }
        return defaultValue;
    }

    public static String checkBoolAttribute(String propertyName, String val, String defaultValue, ProblemReporter reporter, String fileName) {
        if (val != null) {
            if (isBool(val)) {
                return val;
            } else if (reporter != null) {
                reporter.warn("[" + propertyName + "]'s value " + val + " in " + fileName + " is illegal, use " + defaultValue + " replaced");
            }
        }
        return defaultValue;
    }


    public static String getAndSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sync-key-variables");
        try {
            String msg = null;
            if (dbGroups.size() == 0) {
                //with no dbGroups, do not check the variables
                return null;
            }
            Map<String, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(dbGroups.size());
            getAndSyncKeyVariablesForDataSources(dbGroups, keyVariablesTaskMap, needSync);

            boolean lowerCase = false;
            boolean isFirst = true;
            Set<String> firstGroup = new HashSet<>();
            Set<String> secondGroup = new HashSet<>();
            int minNodePacketSize = Integer.MAX_VALUE;
            for (Map.Entry<String, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
                String dataSourceName = entry.getKey();
                Future<KeyVariables> future = entry.getValue();
                KeyVariables keyVariables = future.get();
                if (keyVariables != null) {
                    if (isFirst) {
                        lowerCase = keyVariables.isLowerCase();
                        isFirst = false;
                        firstGroup.add(dataSourceName);
                    } else if (keyVariables.isLowerCase() != lowerCase) {
                        secondGroup.add(dataSourceName);
                    }
                    minNodePacketSize = minNodePacketSize < keyVariables.getMaxPacketSize() ? minNodePacketSize : keyVariables.getMaxPacketSize();
                }
            }
            if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
                SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
                msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
                LOGGER.warn(msg);
            }
            if (secondGroup.size() != 0) {
                // if all datasoure's lower case are not equal, throw exception
                StringBuilder sb = new StringBuilder("The values of lower_case_table_names for backend MySQLs are different.");
                String firstGroupValue;
                String secondGroupValue;
                if (lowerCase) {
                    firstGroupValue = " not 0 :";
                    secondGroupValue = " 0 :";
                } else {
                    firstGroupValue = " 0 :";
                    secondGroupValue = " not 0 :";
                }
                sb.append("These MySQL's value is");
                sb.append(firstGroupValue);
                sb.append(Strings.join(firstGroup, ','));
                sb.append(".And these MySQL's value is");
                sb.append(secondGroupValue);
                sb.append(Strings.join(secondGroup, ','));
                sb.append(".");
                throw new IOException(sb.toString());
            }
            return msg;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }


    private static void getAndSyncKeyVariablesForDataSources(Map<String, PhysicalDbGroup> dbGroups, Map<String, Future<KeyVariables>> keyVariablesTaskMap, boolean needSync) throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(dbGroups.size());
        for (Map.Entry<String, PhysicalDbGroup> entry : dbGroups.entrySet()) {
            String hostName = entry.getKey();
            PhysicalDbGroup pool = entry.getValue();

            for (PhysicalDbInstance ds : pool.getDbInstances(true)) {
                if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                    continue;
                }
                getKeyVariablesForDataSource(service, ds, hostName, keyVariablesTaskMap, needSync);
            }
        }
        service.shutdown();
        int i = 0;
        while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
            if (LOGGER.isDebugEnabled()) {
                if (i == 0) {
                    LOGGER.debug("wait to get all dbInstances's get key variable");
                }
                i++;
                if (i == 100) { //log every 10 seconds
                    i = 0;
                }
            }
        }
    }

    private static void getKeyVariablesForDataSource(ExecutorService service, PhysicalDbInstance ds, String hostName, Map<String, Future<KeyVariables>> keyVariablesTaskMap, boolean needSync) {
        String dataSourceName = genDataSourceKey(hostName, ds.getName());
        GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(ds, needSync);
        Future<KeyVariables> future = service.submit(task);
        keyVariablesTaskMap.put(dataSourceName, future);
    }

    private static String genDataSourceKey(String hostName, String dsName) {
        return hostName + ":" + dsName;
    }
}
