/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.SplitUtil;
import com.alibaba.druid.wall.WallConfig;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author mycat
 */

public class XMLServerLoader {
    private final SystemConfig system;
    private final Map<String, UserConfig> users;
    private final FirewallConfig firewall;

    public XMLServerLoader() {
        this.system = new SystemConfig();
        this.users = new HashMap<>();
        this.firewall = new FirewallConfig();
        this.load();
    }

    public SystemConfig getSystem() {
        return system;
    }

    @SuppressWarnings("unchecked")
    public Map<String, UserConfig> getUsers() {
        return (Map<String, UserConfig>) (users.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(users));
    }

    public FirewallConfig getFirewall() {
        return firewall;
    }


    private void load() {
        //read server.xml
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream("/server.dtd");
            xml = ResourceUtil.getResourceAsStream("/server.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();

            loadSystem(root);
            loadUsers(root);
            loadFirewall(root);
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

    private void loadFirewall(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("host");
        Map<String, List<UserConfig>> whitehost = new HashMap<>();

        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String host = e.getAttribute("host").trim();
                String userStr = e.getAttribute("user").trim();
                if (this.firewall.existsHost(host)) {
                    throw new ConfigException("host duplicated : " + host);
                }
                String[] arrayUsers = userStr.split(",");
                List<UserConfig> userConfigs = new ArrayList<>();
                for (String user : arrayUsers) {
                    UserConfig uc = this.users.get(user);
                    if (null == uc) {
                        throw new ConfigException("[user: " + user + "] doesn't exist in [host: " + host + "]");
                    }
                    if (!uc.isManager() && (uc.getSchemas() == null || uc.getSchemas().size() == 0)) {
                        throw new ConfigException("[host: " + host + "] contains one root privileges user: " + user);
                    }
                    userConfigs.add(uc);
                }
                whitehost.put(host, userConfigs);
            }
        }

        firewall.setWhitehost(whitehost);

        WallConfig wallConfig = new WallConfig();
        NodeList blacklist = root.getElementsByTagName("blacklist");
        for (int i = 0, n = blacklist.getLength(); i < n; i++) {
            Node node = blacklist.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String check = e.getAttribute("check");
                if (null != check) {
                    firewall.setBlackListCheck(Boolean.parseBoolean(check));
                }

                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(wallConfig, props);
            }
        }
        firewall.setWallConfig(wallConfig);
        firewall.init();

    }

    private void loadUsers(Element root) {
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                UserConfig user = new UserConfig();
                Map<String, Object> props = ConfigUtil.loadElements(e);
                String password = (String) props.get("password");
                String usingDecrypt = (String) props.get("usingDecrypt");
                String passwordDecrypt = DecryptUtil.decrypt(usingDecrypt, name, password);
                user.setName(name);
                user.setPassword(passwordDecrypt);
                user.setEncryptPassword(password);

                String benchmark = (String) props.get("benchmark");
                if (null != benchmark) {
                    user.setBenchmark(Integer.parseInt(benchmark));
                }

                String readOnly = (String) props.get("readOnly");
                if (null != readOnly) {
                    user.setReadOnly(Boolean.parseBoolean(readOnly));
                }

                String manager = (String) props.get("manager");
                if (null != manager) {
                    user.setManager(Boolean.parseBoolean(manager));
                }
                String schemas = (String) props.get("schemas");
                if (user.isManager() && schemas != null) {
                    throw new ConfigException("manager user can't set any schema!");
                } else if (!user.isManager()) {
                    if (schemas != null) {
                        if (system.isLowerCaseTableNames()) {
                            schemas = schemas.toLowerCase();
                        }
                        String[] strArray = SplitUtil.split(schemas, ',', true);
                        user.setSchemas(new HashSet<>(Arrays.asList(strArray)));
                    }
                    // load DML
                    loadPrivileges(user, e);
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                users.put(name, user);
            }
        }
    }

    private void loadPrivileges(UserConfig userConfig, Element node) {

        UserPrivilegesConfig privilegesConfig = new UserPrivilegesConfig();

        NodeList privilegesNodes = node.getElementsByTagName("privileges");
        int privilegesNodesLength = privilegesNodes.getLength();
        for (int i = 0; i < privilegesNodesLength; i++) {
            Element privilegesNode = (Element) privilegesNodes.item(i);
            String check = privilegesNode.getAttribute("check");
            if (null != check) {
                privilegesConfig.setCheck(Boolean.valueOf(check));
            }


            NodeList schemaNodes = privilegesNode.getElementsByTagName("schema");
            int schemaNodeLength = schemaNodes.getLength();

            for (int j = 0; j < schemaNodeLength; j++) {
                Element schemaNode = (Element) schemaNodes.item(j);
                String name1 = schemaNode.getAttribute("name");
                if (system.isLowerCaseTableNames()) {
                    name1 = name1.toLowerCase();
                }
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
                    if (system.isLowerCaseTableNames()) {
                        name2 = name2.toLowerCase();
                    }
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

    private void loadSystem(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("system");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(system, props);
            }
        }

        if (system.getFakeMySQLVersion() != null) {
            boolean validVersion = false;
            String majorMySQLVersion = system.getFakeMySQLVersion();
            int pos = majorMySQLVersion.indexOf(".") + 1;
            majorMySQLVersion = majorMySQLVersion.substring(0, majorMySQLVersion.indexOf(".", pos));
            for (String ver : SystemConfig.MYSQL_VERSIONS) {
                // version is x.y.z ,just compare the x.y
                if (majorMySQLVersion.equals(ver)) {
                    validVersion = true;
                }
            }

            if (validVersion) {
                Versions.setServerVersion(system.getFakeMySQLVersion());
            } else {
                throw new ConfigException("The specified MySQL Version (" + system.getFakeMySQLVersion() + ") is not valid.");
            }
        }
    }

}
