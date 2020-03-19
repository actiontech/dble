/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.helper.GetAndSyncDataSourceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.DataSourceConfig;
import com.actiontech.dble.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
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
        int propStart = -1;
        int propStop = -1;
        String propName = null;
        String propValue = null;
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

    public static Map<String, Object> loadAttributes(Element e) {
        Map<String, Object> map = new HashMap<>();
        NamedNodeMap nm = e.getAttributes();
        for (int j = 0; j < nm.getLength(); j++) {
            Node n = nm.item(j);
            if (n instanceof Attr) {
                Attr attr = (Attr) n;
                map.put(attr.getName(), attr.getNodeValue());
            }
        }
        return map;
    }

    public static Element loadElement(Element parent, String tagName) {
        NodeList nodeList = parent.getElementsByTagName(tagName);
        if (nodeList.getLength() > 1) {
            throw new ConfigException(tagName + " elements length  over one!");
        }
        if (nodeList.getLength() == 1) {
            return (Element) nodeList.item(0);
        } else {
            return null;
        }
    }

    /**
     * @param parent
     * @return key-value property
     */
    public static Map<String, Object> loadElements(Element parent) {
        Map<String, Object> map = new HashMap<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getNodeName();
                if ("property".equals(name)) {
                    String key = e.getAttribute("name");
                    String value = e.getTextContent();
                    map.put(key, StringUtil.isEmpty(value) ? null : value.trim());
                }
            }
        }
        return map;
    }

    public static void setSchemasForPool(Map<String, PhysicalDataHost> dataHostMap, Map<String, PhysicalDataNode> dataNodeMap) {
        for (PhysicalDataHost dataHost : dataHostMap.values()) {
            dataHost.setSchemas(getDataNodeSchemasOfDataHost(dataHost.getHostName(), dataNodeMap));
        }
    }

    private static String[] getDataNodeSchemasOfDataHost(String dataHost, Map<String, PhysicalDataNode> dataNodeMap) {
        ArrayList<String> schemaList = new ArrayList<>(30);
        for (PhysicalDataNode dn : dataNodeMap.values()) {
            if (dn.getDataHost().getHostName().equals(dataHost)) {
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
     * @param element
     * @param attrName
     * @param defaultValue
     * @param reporter
     * @return
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

    public static String checkAndGetAttribute(String propertyName, String val, String defaultValue, ProblemReporter reporter) {
        if (val != null) {
            if (isBool(val) || isNumeric(val)) {
                return val;
            } else if (reporter != null) {
                reporter.warn("property[" + propertyName + "] " + val + " in server.xml is illegal, use " + defaultValue + " replaced");
            }
        }
        return defaultValue;
    }


    public static void getAndSyncKeyVariables(boolean isStart, Map<String, PhysicalDataHost> dataHosts) throws Exception {
        Map<String, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(dataHosts.size());
        getAndSyncKeyVariablesForDataSources(isStart, dataHosts, keyVariablesTaskMap);

        boolean lowerCase = false;
        boolean isFirst = true;
        Set<String> firstGroup = new HashSet<>();
        Set<String> secondGroup = new HashSet<>();
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
            }
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
    }



    private static void getAndSyncKeyVariablesForDataSources(boolean isStart, Map<String, PhysicalDataHost> dataHosts, Map<String, Future<KeyVariables>> keyVariablesTaskMap) throws InterruptedException {
        if (dataHosts.size() == 0) {
            return;
        }
        ExecutorService service = Executors.newFixedThreadPool(dataHosts.size());
        for (Map.Entry<String, PhysicalDataHost> entry : dataHosts.entrySet()) {
            String hostName = entry.getKey();
            PhysicalDataHost pool = entry.getValue();

            if (isStart) {
                // start for first time, 1.you can set write host as empty
                if (pool.getWriteSource() == null) {
                    continue;
                }
                DataSourceConfig wHost = pool.getWriteSource().getConfig();
                // start for first time, 2.you can set write host as yourself
                if (("localhost".equalsIgnoreCase(wHost.getIp()) || "127.0.0.1".equalsIgnoreCase(wHost.getIp())) &&
                        wHost.getPort() == DbleServer.getInstance().getConfig().getSystem().getServerPort()) {
                    continue;
                }
            }
            for (PhysicalDataSource ds : pool.getAllDataSources()) {
                if (ds.isDisabled() || !ds.isTestConnSuccess()) {
                    continue;
                }
                getKeyVariablesForDataSource(service, ds, hostName, keyVariablesTaskMap);
            }
        }
        service.shutdown();
        int i = 0;
        while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
            if (LOGGER.isDebugEnabled()) {
                if (i == 0) {
                    LOGGER.debug("wait get all datasouce's get key variable");
                }
                i++;
                if (i == 100) { //log every 10 seconds
                    i = 0;
                }
            }
        }
    }

    private static void getKeyVariablesForDataSource(ExecutorService service, PhysicalDataSource ds, String hostName, Map<String, Future<KeyVariables>> keyVariablesTaskMap) {
        String dataSourceName = genDataSourceKey(hostName, ds.getName());
        GetAndSyncDataSourceKeyVariables task = new GetAndSyncDataSourceKeyVariables(ds);
        Future<KeyVariables> future = service.submit(task);
        keyVariablesTaskMap.put(dataSourceName, future);
    }

    private static String genDataSourceKey(String hostName, String dsName) {
        return hostName + ":" + dsName;
    }
}
