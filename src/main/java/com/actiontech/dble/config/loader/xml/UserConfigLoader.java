/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class UserConfigLoader implements Loader<UserConfig, XMLServerLoader> {
    public void load(Element root, XMLServerLoader xsl) throws IllegalAccessException, InvocationTargetException {
        Map<String, UserConfig> users = xsl.getUsers();
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

                String maxCon = (String) props.get("maxCon");
                if (null != maxCon) {
                    user.setMaxCon(Integer.parseInt(maxCon));
                }

                String readOnly = (String) props.get("readOnly");
                if (null != readOnly) {
                    user.setReadOnly(Boolean.parseBoolean(readOnly));
                }

                String manager = (String) props.get("manager");
                if (null != manager) {
                    user.setManager(Boolean.parseBoolean(manager));
                    user.setSchemas(new HashSet<String>(0));
                }
                String schemas = (String) props.get("schemas");
                if (user.isManager() && schemas != null) {
                    throw new ConfigException("manager user can't set any schema!");
                } else if (!user.isManager()) {
                    if (schemas != null) {
                        String[] strArray = SplitUtil.split(schemas, ',', true);
                        user.setSchemas(new HashSet<>(Arrays.asList(strArray)));
                    }
                    // load DML
                    loadPrivileges(user, false, e);
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                users.put(name, user);
            }
        }
    }

    private void loadPrivileges(UserConfig userConfig, boolean isLowerCaseTableNames, Element node) {
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
                if (isLowerCaseTableNames) {
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
                    if (isLowerCaseTableNames) {
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
}
