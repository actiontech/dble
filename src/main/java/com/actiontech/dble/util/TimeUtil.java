/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

/**
 * weak accuracy timer for performance.
 *
 * @author mycat
 */
public final class TimeUtil {
    private TimeUtil() {
    }

    private static volatile long currentTime = System.currentTimeMillis();

    public static long currentTimeMillis() {
        return currentTime;
    }

    public static long currentTimeNanos() {
        return System.nanoTime();
    }

    public static void update() {
        currentTime = System.currentTimeMillis();
    }

}
