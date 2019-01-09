/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.StringUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class SystemConfigLoader implements Loader<SystemConfig, XMLServerLoader> {
    public void load(Element root, XMLServerLoader xsl) throws IllegalAccessException, InvocationTargetException {
        SystemConfig system = xsl.getSystem();
        NodeList list = root.getElementsByTagName("system");

        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(system, props, xsl.problemReporter);
                if (props.size() > 0) {
                    String[] propItem = new String[props.size()];
                    props.keySet().toArray(propItem);
                    throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propItem, ","));
                }
            }
        }

        if (system.getFakeMySQLVersion() != null) {
            boolean validVersion = false;
            String majorMySQLVersion = system.getFakeMySQLVersion();
            String[] versions = majorMySQLVersion.split("\\.");
            if (versions.length == 3) {
                majorMySQLVersion = versions[0] + "." + versions[1];
                for (String ver : SystemConfig.MYSQL_VERSIONS) {
                    // version is x.y.z ,just compare the x.y
                    if (majorMySQLVersion.equals(ver)) {
                        validVersion = true;
                    }
                }
            }

            if (validVersion) {
                Versions.setServerVersion(system.getFakeMySQLVersion());
            } else {
                throw new ConfigException("The specified MySQL Version (" + system.getFakeMySQLVersion() + ") is not valid, " +
                        "the version should look like 'x.y.z'.");
            }
        }
    }
}
