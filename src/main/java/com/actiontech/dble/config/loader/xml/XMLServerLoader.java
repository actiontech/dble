/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

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

    public XMLServerLoader() {
        this.system = new SystemConfig();
        this.users = new HashMap<>();
        this.firewall = new FirewallConfig();

        this.load(new SystemConfigLoader());
        this.load(new UserConfigLoader());
        this.load(new FirewallConfigLoader());
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

    public void load(Loader loader) {
        //read server.xml
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ResourceUtil.getResourceAsStream("/server.dtd");
            xml = ResourceUtil.getResourceAsStream("/server.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loader.load(root, this);
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
}
