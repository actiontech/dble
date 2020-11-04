/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.user.*;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.*;
import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import com.google.common.collect.Maps;
import org.w3c.dom.*;

import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static com.actiontech.dble.services.manager.information.tables.DbleRwSplitEntry.*;

public class XMLUserLoader {
    public static final String TYPE_MANAGER_USER = "managerUser";
    public static final String TYPE_SHARDING_USER = "shardingUser";
    public static final String TYPE_RWSPLIT_USER = "rwSplitUser";
    private final Map<UserName, UserConfig> users;
    private final Map<String, Properties> blacklistConfig;
    private static final String DEFAULT_DTD = "/user.dtd";
    private static final String DEFAULT_DTD_NAME = "user.dtd";
    private static final String DEFAULT_XML = "/" + ConfigFileName.USER_XML;
    private ProblemReporter problemReporter;
    private AtomicInteger userId = new AtomicInteger(0);
    private static final Pattern DML_PATTERN = Pattern.compile("^[0|1]{4}$");
    private Document document;
    // whether db.xml contains shardingUser
    private boolean containsShardingUser;

    public XMLUserLoader() {
        this.users = Maps.newHashMap();
        this.blacklistConfig = Maps.newHashMap();
        this.problemReporter = null;
        loadXmlDocument(DEFAULT_DTD, DEFAULT_XML);
    }

    public XMLUserLoader(String xmlFile, ProblemReporter problemReporter) {
        this.problemReporter = problemReporter;
        this.users = new HashMap<>();
        this.blacklistConfig = new HashMap<>();
        loadXml(DEFAULT_DTD, xmlFile == null ? DEFAULT_XML : xmlFile);
    }

    @SuppressWarnings("unchecked")
    public Map<UserName, UserConfig> getUsers() {
        return users;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Properties> getBlacklistConfig() {
        return blacklistConfig;
    }

    private void loadXml(String dtdFile, String xmlFile) {
        //read user.xml
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream(dtdFile);
            xml = ResourceUtil.getResourceAsStream(xmlFile);
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            checkVersion(root);
            loadManagerUser(root, xmlFile);
            Map<String, WallProvider> blackListMap = loadBlackList(root);
            loadShardingUser(root, xmlFile, blackListMap);
            loadRwSplitUser(root, xmlFile, blackListMap);
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

    private void loadXmlDocument(String dtdFile, String xmlFile) {
        //read user.xml
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream(dtdFile);
            xml = ResourceUtil.getResourceAsStream(xmlFile);
            this.document = ConfigUtil.getDocument(dtd, xml);
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

    private void loadManagerUser(Element root, String xmlFile) {
        NodeList list = root.getElementsByTagName(TYPE_MANAGER_USER);
        int size = list.getLength();
        if (size == 0) {
            return;
        }

        for (int i = 0; i < size; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                UserConfig baseInfo = getBaseUserInfo(element, xmlFile);
                UserName user = new UserName(baseInfo.getName());
                if (users.containsKey(user)) {
                    throw new ConfigException("User [name:" + baseInfo.getName() + "] has already existed");
                }
                String readOnlyStr = element.getAttribute("readOnly");
                boolean readOnly = false;
                if (!StringUtil.isEmpty(readOnlyStr)) {
                    readOnlyStr = ConfigUtil.checkBoolAttribute("readOnly", readOnlyStr, "false", problemReporter, xmlFile);
                    readOnly = Boolean.parseBoolean(readOnlyStr);
                }

                ManagerUserConfig managerUser = new ManagerUserConfig(baseInfo, readOnly);
                managerUser.setId(userId.incrementAndGet());
                users.put(user, managerUser);
            }
        }
    }

    private void loadShardingUser(Element root, String xmlFile, Map<String, WallProvider> blackListMap) {
        NodeList list = root.getElementsByTagName(TYPE_SHARDING_USER);
        int size = list.getLength();
        if (size == 0) {
            return;
        }

        this.containsShardingUser = true;
        for (int i = 0; i < size; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                UserConfig baseInfo = getBaseUserInfo(element, xmlFile);

                String userName = baseInfo.getName();
                UserName user = new UserName(userName, element.getAttribute("tenant"));
                if (users.containsKey(user)) {
                    throw new ConfigException("User [" + user + "] has already existed");
                }
                String readOnlyStr = element.getAttribute("readOnly");
                boolean readOnly = false;
                if (!StringUtil.isEmpty(readOnlyStr)) {
                    readOnlyStr = ConfigUtil.checkBoolAttribute("readOnly", readOnlyStr, "false", problemReporter, xmlFile);
                    readOnly = Boolean.parseBoolean(readOnlyStr);
                }

                String schemas = element.getAttribute("schemas").trim();
                if (StringUtil.isEmpty(schemas)) {
                    throw new ConfigException("User [" + user + "]'s schemas is empty");
                }
                String[] strArray = SplitUtil.split(schemas, ',', true);

                String blacklist = element.getAttribute("blacklist");
                WallProvider wallProvider = null;
                if (!StringUtil.isEmpty(blacklist)) {
                    wallProvider = blackListMap.get(blacklist);
                    if (wallProvider == null) {
                        problemReporter.warn("blacklist[" + blacklist + "] for user [" + user + "]  is not found, it will be ignore");
                    } else {
                        wallProvider.setName(blacklist);
                    }
                }
                // load DML Privileges
                UserPrivilegesConfig privilegesConfig = loadPrivileges(element, user);

                ShardingUserConfig shardingUser = new ShardingUserConfig(baseInfo, user.getTenant(), wallProvider, readOnly, new HashSet<>(Arrays.asList(strArray)), privilegesConfig);
                shardingUser.setId(userId.incrementAndGet());
                users.put(user, shardingUser);
            }
        }
    }

    private void loadRwSplitUser(Element root, String xmlFile, Map<String, WallProvider> blackListMap) {
        NodeList list = root.getElementsByTagName(TYPE_RWSPLIT_USER);
        int size = list.getLength();
        if (size == 0) {
            return;
        }
        for (int i = 0; i < size; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                UserConfig baseInfo = getBaseUserInfo(element, xmlFile);
                String userName = baseInfo.getName();
                UserName user = new UserName(userName, element.getAttribute("tenant"));
                if (users.containsKey(user)) {
                    throw new ConfigException("User [" + user + "] has already existed");
                }
                String dbGroup = element.getAttribute("dbGroup").trim();
                if (StringUtil.isEmpty(dbGroup)) {
                    throw new ConfigException("User [" + user + "]'s dbGroup is empty");
                }

                String blacklist = element.getAttribute("blacklist");
                WallProvider wallProvider = null;
                if (!StringUtil.isEmpty(blacklist)) {
                    wallProvider = blackListMap.get(blacklist);
                    if (wallProvider == null) {
                        problemReporter.warn("blacklist[" + blacklist + "] for user [" + user + "]  is not found, it will be ignore");
                    } else {
                        wallProvider.setName(blacklist);
                    }
                }

                RwSplitUserConfig rwSplitUser = new RwSplitUserConfig(baseInfo, user.getTenant(), wallProvider, dbGroup);
                rwSplitUser.setId(userId.incrementAndGet());
                users.put(user, rwSplitUser);
            }
        }
    }

    public void insertRwSplitUser(List<LinkedHashMap<String, String>> userConfigList, String xmlFilePath) throws SQLException {
        if (null == userConfigList || userConfigList.isEmpty()) {
            return;
        }
        try {
            for (LinkedHashMap<String, String> userConfigMap : userConfigList) {
                //check unique
                NodeList rwSplitUserNodeList = this.document.getElementsByTagName(TYPE_RWSPLIT_USER);
                String nameKey = "name";
                String tenantKey = "tenant";
                for (int i = 0; i < rwSplitUserNodeList.getLength(); i++) {
                    Node item = rwSplitUserNodeList.item(i);
                    NamedNodeMap attributes = item.getAttributes();
                    Node name = attributes.getNamedItem(nameKey);
                    Node tenant = attributes.getNamedItem(tenantKey);
                    String nameVal = null == name ? null : name.getNodeValue();
                    String tenantVal = null == tenant ? null : tenant.getNodeValue();
                    if (StringUtil.equals(nameVal, userConfigMap.get(nameKey)) && StringUtil.equals(tenantVal, userConfigMap.get(tenantKey))) {
                        String msg = String.format("Duplicate entry '%s-%s-%s'for logical unique '%s-%s-%s'", nameVal,
                                StringUtil.isEmpty(tenantVal) ? null : tenantKey, tenantVal, COLUMN_USERNAME, COLUMN_CONN_ATTR_KEY, COLUMN_CONN_ATTR_VALUE);
                        throw new SQLException(msg, "42S22", ErrorCode.ER_DUP_ENTRY);
                    }
                }
                //insert
                Element rwSplitUserElement = this.document.createElement(TYPE_RWSPLIT_USER);
                userConfigMap.forEach((key, value) -> {
                    if (null != value && !value.isEmpty() && !value.equals("0")) {
                        rwSplitUserElement.setAttribute(key, value);
                    }
                });
                Element root = this.document.getDocumentElement();
                NodeList refNode = this.document.getElementsByTagName("blacklist");
                if (null == refNode || refNode.getLength() == 0) {
                    root.appendChild(rwSplitUserElement);
                } else {
                    root.insertBefore(rwSplitUserElement, refNode.item(refNode.getLength() - 1));
                }
            }
            //backup
            File tempFile = new File(xmlFilePath + ".tmp");
            File file = new File(xmlFilePath);
            FileUtils.copy(file, tempFile);
            XmlUtil.saveDocument(this.document, xmlFilePath, DEFAULT_DTD_NAME, true);
        } catch (TransformerException | IOException e) {
            throw new ConfigException(e);
        }
    }

    public void updateRwSplitUser(List<LinkedHashMap<String, String>> userConfigList, Map<String, String> values, String xmlFilePath) {
        if (null == userConfigList || userConfigList.isEmpty() || null == values || values.isEmpty()) {
            return;
        }
        boolean isExist = false;
        try {
            for (LinkedHashMap<String, String> userConfigMap : userConfigList) {
                NodeList rwSplitUserNodeList = this.document.getElementsByTagName(TYPE_RWSPLIT_USER);
                for (int i = 0; i < rwSplitUserNodeList.getLength(); i++) {
                    Node item = rwSplitUserNodeList.item(i);
                    NamedNodeMap attributes = item.getAttributes();
                    Node name = attributes.getNamedItem("name");
                    Node tenant = attributes.getNamedItem("tenant");
                    String nameVal = null == name ? null : name.getNodeValue();
                    String tenantVal = null == tenant ? null : tenant.getNodeValue();
                    if (StringUtil.equals(nameVal, userConfigMap.get("name")) && StringUtil.equals(tenantVal, userConfigMap.get("tenant"))) {
                        isExist = true;
                        Element itemElement = (Element) item;
                        for (Map.Entry<String, String> entry : values.entrySet()) {
                            itemElement.setAttribute(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
            if (!isExist) {
                return;
            }
            //backup
            File tempFile = new File(xmlFilePath + ".tmp");
            File file = new File(xmlFilePath);
            FileUtils.copy(file, tempFile);
            XmlUtil.saveDocument(this.document, xmlFilePath, DEFAULT_DTD_NAME, true);
        } catch (TransformerException | IOException e) {
            throw new ConfigException(e);
        }
    }

    public void deleteRwSplitUser(List<LinkedHashMap<String, String>> userConfigList, String xmlFilePath) {
        if (null == userConfigList || userConfigList.isEmpty()) {
            return;
        }
        boolean isExist = false;
        try {
            for (LinkedHashMap<String, String> userConfigMap : userConfigList) {
                NodeList rwSplitUserNodeList = this.document.getElementsByTagName(TYPE_RWSPLIT_USER);
                String nameKey = "name";
                String tenantKey = "tenant";
                for (int i = 0; i < rwSplitUserNodeList.getLength(); i++) {
                    Node item = rwSplitUserNodeList.item(i);
                    NamedNodeMap attributes = item.getAttributes();
                    Node name = attributes.getNamedItem(nameKey);
                    Node tenant = attributes.getNamedItem(tenantKey);
                    String nameVal = null == name ? null : name.getNodeValue();
                    String tenantVal = null == tenant ? null : tenant.getNodeValue();
                    if (StringUtil.equals(nameVal, userConfigMap.get(nameKey)) && StringUtil.equals(tenantVal, userConfigMap.get(tenantKey))) {
                        isExist = true;
                        item.getParentNode().removeChild(item);
                    }
                }
            }
            if (!isExist) {
                return;
            }
            //backup
            File tempFile = new File(xmlFilePath + ".tmp");
            File file = new File(xmlFilePath);
            FileUtils.copy(file, tempFile);
            XmlUtil.saveDocument(this.document, xmlFilePath, DEFAULT_DTD_NAME, true);
        } catch (TransformerException | IOException e) {
            throw new ConfigException(e);
        }
    }


    private Map<String, WallProvider> loadBlackList(Element root) throws InvocationTargetException, IllegalAccessException {
        NodeList blacklist = root.getElementsByTagName("blacklist");
        if (blacklist.getLength() > 0) {
            Map<String, WallProvider> blackListMap = new HashMap<>(blacklist.getLength());
            for (int i = 0, n = blacklist.getLength(); i < n; i++) {
                WallConfig wallConfig = new WallConfig();
                Node node = blacklist.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    String name = element.getAttribute("name");
                    if (blackListMap.containsKey(name)) {
                        throw new ConfigException("blacklist[" + name + "]  has already existed");
                    }
                    Properties props = ConfigUtil.loadElements(element);
                    Properties props2 = new Properties();
                    props2.putAll(props);
                    blacklistConfig.put(name, props2);
                    ParameterMapping.mapping(wallConfig, props, problemReporter);
                    if (props.size() > 0) {
                        String[] propItem = new String[props.size()];
                        props.keySet().toArray(propItem);
                        throw new ConfigException("blacklist item(s) is not recognized: " + StringUtil.join(propItem, ","));
                    }
                    WallProvider provider = new MySqlWallProvider(wallConfig);
                    provider.setBlackListEnable(true);
                    blackListMap.put(name, provider);
                }
            }
            return blackListMap;
        } else {
            return Collections.emptyMap();
        }

    }

    private UserConfig getBaseUserInfo(Element element, String xmlFile) {
        String name = element.getAttribute("name");
        if (StringUtil.isEmpty(name)) {
            throw new ConfigException("one of users' name is empty");
        }
        String password = element.getAttribute("password");
        if (StringUtil.isEmpty(password)) {
            throw new ConfigException("password of " + name + " is empty");
        }
        boolean usingDecrypt = false;
        String usingDecryptStr = element.getAttribute("usingDecrypt");
        if (!StringUtil.isEmpty(usingDecryptStr)) {
            usingDecryptStr = ConfigUtil.checkBoolAttribute("usingDecrypt", usingDecryptStr, "false", problemReporter, xmlFile);
            usingDecrypt = Boolean.parseBoolean(usingDecryptStr);
            password = DecryptUtil.decrypt(usingDecrypt, name, password);
        }

        String strWhiteIPs = element.getAttribute("whiteIPs");
        String strMaxCon = element.getAttribute("maxCon");
        IPAddressUtil.checkWhiteIPs(strWhiteIPs);
        return new UserConfig(name, password, usingDecrypt, strWhiteIPs, strMaxCon);
    }


    private void checkVersion(Element root) {
        String version = null;
        if (root.getAttributes().getNamedItem("version") != null) {
            version = root.getAttributes().getNamedItem("version").getNodeValue();
        }
        if (version != null && !Versions.CONFIG_VERSION.equals(version)) {
            if (this.problemReporter != null) {
                if (Versions.checkVersion(version)) {
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.USER_XML + " version is " + version + ".There may be some incompatible config between two versions, please check it";
                    this.problemReporter.warn(message);
                } else {
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the " + ConfigFileName.USER_XML + " version is " + version + ".There must be some incompatible config between two versions, please check it";
                    this.problemReporter.warn(message);
                }
            }
        }
    }

    private UserPrivilegesConfig loadPrivileges(Element node, UserName user) {

        NodeList privilegesNodes = node.getElementsByTagName("privileges");
        int privilegesNodesLength = privilegesNodes.getLength();
        if (privilegesNodesLength == 0) {
            return null;
        }
        UserPrivilegesConfig privilegesConfig = new UserPrivilegesConfig();
        for (int i = 0; i < privilegesNodesLength; i++) {
            Element privilegesNode = (Element) privilegesNodes.item(i);
            String checkStr = ConfigUtil.checkAndGetAttribute(privilegesNode, "check", "false", problemReporter);
            boolean check = Boolean.parseBoolean(checkStr);
            privilegesConfig.setCheck(check);

            NodeList schemaNodes = privilegesNode.getElementsByTagName("schema");
            int schemaNodeLength = schemaNodes.getLength();
            for (int j = 0; j < schemaNodeLength; j++) {
                Element schemaNode = (Element) schemaNodes.item(j);
                final String name1 = schemaNode.getAttribute("name");

                String dml1 = schemaNode.getAttribute("dml");
                if (!DML_PATTERN.matcher(dml1).matches())
                    throw new ConfigException("User [" + user + "]'s schema [" + name1 + "]'s privilege's dml is not correct");
                int[] dml1Array = new int[dml1.length()];
                for (int offset1 = 0; offset1 < dml1.length(); offset1++) {
                    dml1Array[offset1] = Character.getNumericValue(dml1.charAt(offset1));
                }

                UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = new UserPrivilegesConfig.SchemaPrivilege();
                schemaPrivilege.setDml(dml1Array);

                NodeList tableNodes = schemaNode.getElementsByTagName("table");
                int tableNodeLength = tableNodes.getLength();
                for (int z = 0; z < tableNodeLength; z++) {
                    UserPrivilegesConfig.TablePrivilege tablePrivilege = new UserPrivilegesConfig.TablePrivilege();
                    Element tableNode = (Element) tableNodes.item(z);
                    final String name2 = tableNode.getAttribute("name");

                    String dml2 = tableNode.getAttribute("dml");
                    if (!DML_PATTERN.matcher(dml2).matches())
                        throw new ConfigException("User [" + user + "]'s schema [" + name1 + "]'s table [" + name2 + "]'s privilege's dml is not correct");
                    int[] dml2Array = new int[dml2.length()];
                    for (int offset2 = 0; offset2 < dml2.length(); offset2++) {
                        dml2Array[offset2] = Character.getNumericValue(dml2.charAt(offset2));
                    }
                    tablePrivilege.setDml(dml2Array);

                    schemaPrivilege.addTablePrivilege(name2, tablePrivilege);
                }

                privilegesConfig.addSchemaPrivilege(name1, schemaPrivilege);
            }
        }
        return privilegesConfig;
    }

    public boolean isContainsShardingUser() {
        return containsShardingUser;
    }

}
