/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import java.io.InputStream;

/**
 * Created by huqing.yan on 2017/7/5.
 */
public final class ResourceUtil {
    private ResourceUtil() {
    }

    public static InputStream getResourceAsStream(String name) {
        return ResourceUtil.class.getResourceAsStream(name);
    }

    public static InputStream getResourceAsStreamForCurrentThread(String name) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    }

    public static String getResourcePathFromRoot(String path) {
        return ResourceUtil.class.getClassLoader().getResource(path).getPath();
    }

    public static InputStream getResourceAsStreamFromRoot(String path) {
        return ResourceUtil.class.getClassLoader().getResourceAsStream(path);
    }
}
