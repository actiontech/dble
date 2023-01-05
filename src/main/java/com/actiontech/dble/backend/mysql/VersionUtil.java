/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql;

import javax.annotation.Nullable;

public final class VersionUtil {
    private static final String VERSION_8 = "8.";
    private static final String VERSION_5 = "5.";

    private VersionUtil() {
    }

    public static final String TX_READ_ONLY = "tx_read_only";
    public static final String TRANSACTION_READ_ONLY = "transaction_read_only";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";

    public static String getIsolationNameByVersion(String version) {
        if (version == null) {
            return null;
        } else {
            final int versionNumber = getMajorVersion(version);
            if (versionNumber == 8) {
                return TRANSACTION_ISOLATION;
            } else {
                return TX_ISOLATION;
            }
        }
    }

    @Nullable
    public static Integer getMajorVersionWithoutDefaultValue(String version) {
        if (version.startsWith(VERSION_8)) {
            return 8;
        } else if (version.startsWith(VERSION_5) || version.contains("MariaDB")) {
            return 5;
        } else {
            return null;
        }
    }

    /**
     * if unrecognized ,will return default value 5 .
     *
     * @param version
     * @return
     */
    public static int getMajorVersion(String version) {
        final Integer versionNumber = VersionUtil.getMajorVersionWithoutDefaultValue(version);
        return versionNumber == null ? 5 : versionNumber;
    }


    public static boolean isMysql8(String version) {
        final Integer versionNumber = VersionUtil.getMajorVersionWithoutDefaultValue(version);
        return versionNumber == 8;
    }

}
