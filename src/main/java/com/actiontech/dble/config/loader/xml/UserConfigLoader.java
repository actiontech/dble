/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
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
                final UserConfig user = new UserConfig();
                Map<String, Object> props = ConfigUtil.loadElements(e);
                String password = (String) props.get("password");
                String usingDecryptStr = (String) props.get("usingDecrypt");
                usingDecryptStr = ConfigUtil.checkAndGetAttribute("usingDecrypt", usingDecryptStr, "0", xsl.problemReporter);
                boolean usingDecrypt = false;
                if ("1".equals(usingDecryptStr)) {
                    usingDecrypt = true;
                } else if (!"0".equals(usingDecryptStr)) {
                    xsl.problemReporter.warn("property[usingDecrypt] " + usingDecryptStr + " in server.xml is illegal, use 0 replaced!");
                }
                String passwordDecrypt = DecryptUtil.decrypt(usingDecrypt, name, password);
                if (passwordDecrypt == null) {
                    throw new ConfigException("User password must be configured");
                } else {
                    props.remove("password");
                    props.remove("usingDecrypt");
                }
                user.setName(name);
                user.setPassword(passwordDecrypt);
                user.setEncryptPassword(password);

                String maxCon = (String) props.get("maxCon");
                if (null != maxCon) {
                    props.remove("maxCon");
                    user.setMaxCon(Integer.parseInt(maxCon));
                }

                String readOnlyStr = (String) props.get("readOnly");
                if (null != readOnlyStr) {
                    props.remove("readOnly");
                    readOnlyStr = ConfigUtil.checkAndGetAttribute("readOnly", readOnlyStr, "false", xsl.problemReporter);
                    user.setReadOnly(Boolean.parseBoolean(readOnlyStr));
                }

                String managerStr = (String) props.get("manager");
                if (null != managerStr) {
                    props.remove("manager");
                    managerStr = ConfigUtil.checkAndGetAttribute("manager", managerStr, "false", xsl.problemReporter);
                    user.setManager(Boolean.parseBoolean(managerStr));
                    user.setSchemas(new HashSet<String>(0));
                }
                String schemas = (String) props.get("schemas");
                if (user.isManager() && schemas != null) {
                    throw new ConfigException("manager user can't set any schema!");
                } else if (!user.isManager()) {
                    props.remove("schemas");
                    if (schemas != null && !"".equals(schemas)) {
                        String[] strArray = SplitUtil.split(schemas, ',', true);
                        user.setSchemas(new HashSet<>(Arrays.asList(strArray)));
                    } else {
                        throw new ConfigException("Server user must have at least one schemas");
                    }
                    // load DML
                    loadPrivileges(user, e, xsl.problemReporter);
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                if (props.size() > 0) {
                    String[] propItem = new String[props.size()];
                    props.keySet().toArray(propItem);
                    throw new ConfigException("These properties of user[" + name + "]  are not recognized: " + StringUtil.join(propItem, ","));
                }
                users.put(name, user);
            }
        }
    }

    private void loadPrivileges(UserConfig userConfig, Element node, ProblemReporter reporter) {
        UserPrivilegesConfig privilegesConfig = new UserPrivilegesConfig();

        NodeList privilegesNodes = node.getElementsByTagName("privileges");
        int privilegesNodesLength = privilegesNodes.getLength();
        for (int i = 0; i < privilegesNodesLength; i++) {
            Element privilegesNode = (Element) privilegesNodes.item(i);
            String checkStr = ConfigUtil.checkAndGetAttribute(privilegesNode, "check", "false", reporter);
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
