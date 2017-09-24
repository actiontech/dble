/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.route.sequence.handler.IncrSequenceHandler;
import com.actiontech.dble.util.ResourceUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * PropertiesUtil
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:08:03 2016/5/3
 */
public final class PropertiesUtil {
    private PropertiesUtil() {
    }

    public static Properties loadProps(String propsFile) {
        Properties props = new Properties();
        InputStream inp = ResourceUtil.getResourceAsStreamForCurrentThread(propsFile);

        if (inp == null) {
            throw new java.lang.RuntimeException("time sequnce properties not found " + propsFile);
        }
        try {
            props.load(inp);
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
        return props;
    }

    public static Properties loadProps(String propsFile, boolean isLowerCaseTableNames) {
        Properties props = loadProps(propsFile);
        if (isLowerCaseTableNames) {
            Properties newProps = new Properties();
            Enumeration<?> enu = props.propertyNames();
            while (enu.hasMoreElements()) {
                String key = (String) enu.nextElement();
                if (key.endsWith(IncrSequenceHandler.KEY_MIN_NAME) || key.endsWith(IncrSequenceHandler.KEY_MAX_NAME) || key.endsWith(IncrSequenceHandler.KEY_CUR_NAME)) {
                    int index = key.lastIndexOf('.');
                    newProps.setProperty(key.substring(0, index).toLowerCase() + key.substring(index), props.getProperty(key));
                } else {
                    newProps.setProperty(key.toLowerCase(), props.getProperty(key));
                }
            }
            props.clear();
            return newProps;
        } else {
            return props;
        }
    }
}
