/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class UserConfigLoader implements Loader<UserConfig, XMLServerLoader> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserConfigLoader.class);

    public void load(Element root, XMLServerLoader xsl) throws IllegalAccessException, InvocationTargetException {
        Map<String, UserConfig> users = xsl.getUsers();
        List<ErrorInfo> errors = xsl.getErrors();

        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                UserConfig user = new UserConfig();
                Map<String, Object> props = ConfigUtil.loadElements(e);
                String password = (String) props.get("password");
                String usingDecryptStr = (String) props.get("usingDecrypt");
                boolean usingDecrypt = false;
                if (usingDecryptStr != null) {
                    if ("1".equals(usingDecryptStr)) {
                        usingDecrypt = true;
                    } else if (!"0".equals(usingDecryptStr)) {
                        String warning = "user[" + name + "] property usingDecrypt " + usingDecryptStr + " is not recognized, use 0 replaced";
                        LOGGER.warn(warning);
                        errors.add(new ErrorInfo("Xml", "WARNING", warning));
                    }
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

                String readOnly = (String) props.get("readOnly");
                if (null != readOnly) {
                    props.remove("readOnly");
                    Boolean readOnlyBool = BooleanUtils.toBooleanObject(readOnly);
                    if (readOnlyBool == null) {
                        readOnlyBool = false;
                        String warning = "user[" + name + "] property readOnly " + readOnly + " is not recognized, use false replaced";
                        LOGGER.warn(warning);
                        errors.add(new ErrorInfo("Xml", "WARNING", warning));
                    }
                    user.setReadOnly(readOnlyBool);
                }

                String manager = (String) props.get("manager");
                if (null != manager) {
                    props.remove("manager");
                    Boolean managerBool = BooleanUtils.toBooleanObject(manager);
                    if (null == managerBool) {
                        managerBool = false;
                        String warning = "user[" + name + "] property manager " + manager + " is not recognized, use false replaced";
                        LOGGER.warn(warning);
                        errors.add(new ErrorInfo("Xml", "WARNING", warning));
                    }
                    user.setManager(managerBool);
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
                    loadPrivileges(user, e, errors);
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

    private void loadPrivileges(UserConfig userConfig, Element node, List<ErrorInfo> errors) {
        UserPrivilegesConfig privilegesConfig = new UserPrivilegesConfig();

        NodeList privilegesNodes = node.getElementsByTagName("privileges");
        int privilegesNodesLength = privilegesNodes.getLength();
        for (int i = 0; i < privilegesNodesLength; i++) {
            Element privilegesNode = (Element) privilegesNodes.item(i);
            if (privilegesNode.hasAttribute("check")) {
                Boolean check = BooleanUtils.toBooleanObject(privilegesNode.getAttribute("check"));
                if (null == check) {
                    check = false;
                    String warning = "user[" + userConfig.getName() + "] privileges check is not recognized, use false replaced!";
                    LOGGER.warn(warning);
                    errors.add(new ErrorInfo("Xml", "WARNING", warning));
                }
                privilegesConfig.setCheck(check);
            }

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
