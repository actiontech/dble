/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.util;

import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.JsonObjectWriter;
import com.oceanbase.obsharding_d.route.sequence.handler.IncrSequenceHandler;
import com.oceanbase.obsharding_d.util.ResourceUtil;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);


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

    public static JsonObject getOrderedMap(String propsFile) {
        try (InputStream inp = ResourceUtil.getResourceAsStreamForCurrentThread(propsFile)) {
            if (inp == null) {
                throw new java.lang.RuntimeException("sequence properties not found " + propsFile);
            }
            final JsonObjectWriter mp = new JsonObjectWriter();
            (new Properties() {
                @Override
                public synchronized Object put(Object key, Object value) {
                    mp.addProperty((String) key, (String) value);
                    return value;
                }
            }).load(inp);
            return mp.toJsonObject();
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

    public static void storeProps(Properties props, String propsFile) throws IOException {
        OutputStream os = null;
        try {
            os = new FileOutputStream(new File(ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH)).getPath() + File.separator + propsFile);
            props.store(os, "\n Copyright (C) 2016-2023 ActionTech.\n License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.\n");
        } catch (IOException e) {
            throw e;
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    LOGGER.warn("close error", e);
                }
            }
        }
    }
}
