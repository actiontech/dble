/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.util;

import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author yanglixue
 */
public final class DnPropertyUtil {
    private DnPropertyUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger("DnPropertyUtil");

    /**
     * loadDnIndexProps
     *
     * @return Properties
     */
    public static Properties loadDnIndexProps() {
        Properties prop = new Properties();
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
        if (!file.exists()) {
            return prop;
        }
        FileInputStream filein = null;
        try {
            filein = new FileInputStream(file);
            prop.load(filein);
        } catch (Exception e) {
            LOGGER.warn("load DataNodeIndex err:" + e);
        } finally {
            if (filein != null) {
                try {
                    filein.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
        return prop;
    }

}
