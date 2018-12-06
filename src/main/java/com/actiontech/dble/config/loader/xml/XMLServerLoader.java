/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.util.ResourceUtil;
import org.w3c.dom.Element;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */

public class XMLServerLoader {
    private final SystemConfig system;
    private final Map<String, UserConfig> users;
    private final FirewallConfig firewall;
    protected ProblemReporter problemReporter;

    public XMLServerLoader() {
        this(null);
    }

    public XMLServerLoader(ProblemReporter problemReporter) {
        this.problemReporter = problemReporter;
        this.system = new SystemConfig(problemReporter);
        this.users = new HashMap<>();
        this.firewall = new FirewallConfig();
        Element root = loadXml();
        this.load(root, new SystemConfigLoader());
        this.load(root, new UserConfigLoader());
        this.load(root, new FirewallConfigLoader());
    }

    public SystemConfig getSystem() {
        return system;
    }

    @SuppressWarnings("unchecked")
    public Map<String, UserConfig> getUsers() {
        //return (Map<String, UserConfig>) (users.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(users));
        return users;
    }

    public FirewallConfig getFirewall() {
        return firewall;
    }

    public Element loadXml() {
        //read server.xml
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream("/server.dtd");
            xml = ResourceUtil.getResourceAsStream("/server.xml");
            return ConfigUtil.getDocument(dtd, xml).getDocumentElement();
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

    public void load(Element root, Loader loader) {
        try {
            loader.load(root, this);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        }
    }
}
