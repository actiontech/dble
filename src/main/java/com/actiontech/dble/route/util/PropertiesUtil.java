/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.route.sequence.handler.IncrSequenceHandler;
import com.actiontech.dble.util.ResourceUtil;

import java.io.*;
import java.util.*;

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

    public static Properties loadProps(String propsFile, boolean isLowerCaseTableNames) {
        Properties props = loadProps(propsFile);
        return handleLowerCase(props, isLowerCaseTableNames);
    }

    public static Properties loadProps(String propsFile) {
        Properties props = new Properties();
        try (InputStream inp = ResourceUtil.getResourceAsStreamForCurrentThread(propsFile)) {
            if (inp == null) {
                throw new java.lang.RuntimeException("sequence properties not found " + propsFile);
            }
            props.load(inp);
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
        return props;
    }

    public static Map<String, String> getOrderedMap(String propsFile) {
        try (InputStream inp = ResourceUtil.getResourceAsStreamForCurrentThread(propsFile)) {
            if (inp == null) {
                throw new java.lang.RuntimeException("sequence properties not found " + propsFile);
            }
            Map<String, String> mp = new LinkedHashMap<>();
            (new Properties() {
                public synchronized Object put(Object key, Object value) {
                    return mp.put((String) key, (String) value);
                }
            }).load(inp);
            return mp;
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
    }

    public static Properties handleLowerCase(Properties props, boolean isLowerCaseTableNames) {
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

    public static void storeProps(Properties props, String propsFile) {
        try (OutputStream os = new FileOutputStream(new File(ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH)).getPath() + File.separator + propsFile)) {
            props.store(os, "\n Copyright (C) 2016-2020 ActionTech.\n License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.\n");
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
    }
}
