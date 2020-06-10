/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class XMLUserLoader {
    private final Map<Pair<String, String>, UserConfig> users;
    private static final String DEFAULT_DTD = "/user.dtd";
    private static final String DEFAULT_XML = "/user.xml";
    private ProblemReporter problemReporter;

    public XMLUserLoader(String xmlFile, ProblemReporter problemReporter) {
        this.problemReporter = problemReporter;
        this.users = new HashMap<>();
        loadXml(DEFAULT_DTD, xmlFile == null ? DEFAULT_XML : xmlFile);
    }

    @SuppressWarnings("unchecked")
    public Map<Pair<String, String>, UserConfig> getUsers() {
        return users;
    }

    public void loadXml(String dtdFile, String xmlFile) {
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

    private void loadManagerUser(Element root, String xmlFile) {
        NodeList list = root.getElementsByTagName("managerUser");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                String[] baseInfo = getBaseUserInfo(element, xmlFile);
                ManagerUserConfig managerUser = new ManagerUserConfig(baseInfo[0], baseInfo[1], baseInfo[2], baseInfo[3]);
                String userName = managerUser.getName();
                Pair<String, String> user = new Pair<>(userName, "");
                if (users.containsKey(user)) {
                    throw new ConfigException("User [name:" + userName + "] has already existed");
                }
                String readOnlyStr = element.getAttribute("readOnly");
                boolean readOnly = false;
                if (!StringUtil.isEmpty(readOnlyStr)) {
                    readOnlyStr = ConfigUtil.checkBoolAttribute("readOnly", readOnlyStr, "false", problemReporter, xmlFile);
                    readOnly = Boolean.parseBoolean(readOnlyStr);
                }
                managerUser.setReadOnly(readOnly);
                users.put(user, managerUser);
            }
        }
    }

    private void loadShardingUser(Element root, String xmlFile, Map<String, WallProvider> blackListMap) {
        NodeList list = root.getElementsByTagName("shardingUser");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                String[] baseInfo = getBaseUserInfo(element, xmlFile);
                ShardingUserConfig shardingUser = new ShardingUserConfig(baseInfo[0], baseInfo[1], baseInfo[2], baseInfo[3]);
                String userName = shardingUser.getName();
                String tenant = element.getAttribute("tenant");
                Pair<String, String> user = new Pair<>(userName, tenant);
                if (users.containsKey(user)) {
                    throw new ConfigException("User [name:" + userName + ",tenant:" + tenant + "] has already existed");
                }
                shardingUser.setTenant(tenant);
                String readOnlyStr = element.getAttribute("readOnly");
                boolean readOnly = false;
                if (!StringUtil.isEmpty(readOnlyStr)) {
                    readOnlyStr = ConfigUtil.checkBoolAttribute("readOnly", readOnlyStr, "false", problemReporter, xmlFile);
                    readOnly = Boolean.parseBoolean(readOnlyStr);
                }
                shardingUser.setReadOnly(readOnly);

                String schemas = element.getAttribute("schemas").trim();
                if (StringUtil.isEmpty(schemas)) {
                    throw new ConfigException("User[name:" + userName + ",tenant:" + tenant + "]'s schemas is empty");
                }
                String[] strArray = SplitUtil.split(schemas, ',', true);
                shardingUser.setSchemas(new HashSet<>(Arrays.asList(strArray)));

                String blacklist = element.getAttribute("blacklist");
                if (!StringUtil.isEmpty(blacklist)) {
                    WallProvider wallProvider = blackListMap.get(blacklist);
                    if (wallProvider == null) {
                        problemReporter.warn("blacklist[" + blacklist + "] for user [name:" + userName + ",tenant:" + tenant + "]  is not found, it will be ignore");
                    } else {
                        shardingUser.setBlacklist(wallProvider);
                    }
                }
                // load DML Privileges
                loadPrivileges(shardingUser, element);
                users.put(user, shardingUser);
            }
        }
    }

    private void loadRwSplitUser(Element root, String xmlFile, Map<String, WallProvider> blackListMap) {
        NodeList list = root.getElementsByTagName("rwSplitUser");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                String[] baseInfo = getBaseUserInfo(element, xmlFile);
                RwSplitUserConfig rwSplitUser = new RwSplitUserConfig(baseInfo[0], baseInfo[1], baseInfo[2], baseInfo[3]);
                String userName = rwSplitUser.getName();
                String tenant = element.getAttribute("tenant");
                Pair<String, String> user = new Pair<>(userName, tenant);
                if (users.containsKey(user)) {
                    throw new ConfigException("User [name:" + userName + ",tenant:" + tenant + "] has already existed");
                }
                rwSplitUser.setTenant(tenant);

                String dbGroup = element.getAttribute("dbGroup").trim();
                if (StringUtil.isEmpty(dbGroup)) {
                    throw new ConfigException("User[name:" + userName + ",tenant:" + tenant + "]'s dbGroup is empty");
                }
                rwSplitUser.setDbGroup(dbGroup);

                String blacklist = element.getAttribute("blacklist");
                if (!StringUtil.isEmpty(blacklist)) {
                    WallProvider wallProvider = blackListMap.get(blacklist);
                    if (wallProvider == null) {
                        problemReporter.warn("blacklist[" + blacklist + "] for user [name:" + userName + ",tenant:" + tenant + "]  is not found, it will be ignore");
                    } else {
                        rwSplitUser.setBlacklist(wallProvider);
                    }
                }
                users.put(user, rwSplitUser);
            }
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

    /**
     * @param element root
     * @param xmlFile file
     * @return name, password, strWhiteIPs
     */
    private String[] getBaseUserInfo(Element element, String xmlFile) {
        String name = element.getAttribute("name");
        String password = element.getAttribute("password");
        String usingDecryptStr = element.getAttribute("usingDecrypt");
        if (!StringUtil.isEmpty(usingDecryptStr)) {
            usingDecryptStr = ConfigUtil.checkBoolAttribute("usingDecrypt", usingDecryptStr, "false", problemReporter, xmlFile);
            boolean usingDecrypt = Boolean.parseBoolean(usingDecryptStr);
            password = DecryptUtil.decrypt(usingDecrypt, name, password);
        }

        String strWhiteIPs = element.getAttribute("whiteIPs");
        String strMaxCon = element.getAttribute("maxCon");
        return new String[]{name, password, strWhiteIPs, strMaxCon};
    }

    private void checkVersion(Element root) {
        String version = null;
        if (root.getAttributes().getNamedItem("version") != null) {
            version = root.getAttributes().getNamedItem("version").getNodeValue();
        }
        if (version != null && !Versions.CONFIG_VERSION.equals(version)) {
            if (this.problemReporter != null) {
                if (Versions.checkVersion(version)) {
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the user.xml version is " + version + ".There may be some incompatible config between two versions, please check it";
                    this.problemReporter.notice(message);
                } else {
                    String message = "The dble-config-version is " + Versions.CONFIG_VERSION + ",but the user.xml version is " + version + ".There must be some incompatible config between two versions, please check it";
                    this.problemReporter.notice(message);
                }
            }
        }
    }

    private void loadPrivileges(ShardingUserConfig userConfig, Element node) {
        UserPrivilegesConfig privilegesConfig = new UserPrivilegesConfig();

        NodeList privilegesNodes = node.getElementsByTagName("privileges");
        int privilegesNodesLength = privilegesNodes.getLength();
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
                    String name2 = tableNode.getAttribute("name");

                    String dml2 = tableNode.getAttribute("dml");
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
        userConfig.setPrivilegesConfig(privilegesConfig);
    }
}
