/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.*;
import org.xml.sax.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author mycat
 */
public final class ConfigUtil {
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

    public static void setSchemasForPool(Map<String, PhysicalDBPool> dataHostMap, Map<String, PhysicalDBNode> dataNodeMap) {
        for (PhysicalDBPool dbPool : dataHostMap.values()) {
            dbPool.setSchemas(getDataNodeSchemasOfDataHost(dbPool.getHostName(), dataNodeMap));
        }
    }

    private static String[] getDataNodeSchemasOfDataHost(String dataHost, Map<String, PhysicalDBNode> dataNodeMap) {
        ArrayList<String> schemaList = new ArrayList<>(30);
        for (PhysicalDBNode dn : dataNodeMap.values()) {
            if (dn.getDbPool().getHostName().equals(dataHost)) {
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
}
