/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallConfig;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirewallConfigLoader implements Loader<FirewallConfig, XMLServerLoader> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirewallConfigLoader.class);

    public void load(Element root, XMLServerLoader xsl) throws IllegalAccessException, InvocationTargetException {
        FirewallConfig firewall = xsl.getFirewall();
        Map<String, UserConfig> users = xsl.getUsers();
        List<ErrorInfo> errors = xsl.getErrors();

        NodeList list = root.getElementsByTagName("host");
        Map<String, List<UserConfig>> whitehost = new HashMap<>();

        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String host = e.getAttribute("host").trim();
                String userStr = e.getAttribute("user").trim();
                if (firewall.existsHost(host)) {
                    throw new ConfigException("host duplicated : " + host);
                }
                String[] arrayUsers = userStr.split(",");
                List<UserConfig> userConfigs = new ArrayList<>();
                for (String user : arrayUsers) {
                    UserConfig uc = users.get(user);
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
                if (e.hasAttribute("check")) {
                    Boolean check = BooleanUtils.toBooleanObject(e.getAttribute("check"));
                    if (null == check) {
                        check = Boolean.FALSE;
                        String warning = "blacklist attribute check " + e.getAttribute("check") + " in server.xml is not recognized, using false replaced.";
                        LOGGER.warn(warning);
                        errors.add(new ErrorInfo("Xml", "WARNING", warning));
                    }
                    firewall.setBlackListCheck(check);
                }

                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(wallConfig, props);
                if (props.size() > 0) {
                    String[] propItem = new String[props.size()];
                    props.keySet().toArray(propItem);
                    throw new ConfigException("blacklist item(s) is not recognized: " + StringUtil.join(propItem, ","));
                }
            }
        }
        firewall.setWallConfig(wallConfig);
        firewall.init();

    }
}
