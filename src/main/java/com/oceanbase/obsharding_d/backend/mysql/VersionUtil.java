/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql;

import com.oceanbase.obsharding_d.config.model.MysqlVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public final class VersionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(VersionUtil.class);
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
            MysqlVersion mysqlVersion = VersionUtil.parseVersion(version);
            //refer to:com.mysql.jdbc.ConnectionImpl.getTransactionIsolation
            boolean isMatch = (VersionUtil.versionMeetsMinimum(5, 7, 20, mysqlVersion) && !VersionUtil.versionMeetsMinimum(8, 0, 0, mysqlVersion)) ||
                    VersionUtil.versionMeetsMinimum(8, 0, 3, mysqlVersion);
            return isMatch ? TRANSACTION_ISOLATION : TX_ISOLATION;
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

    /**
     * Does the version of the MySQL server we are connected to meet the given
     * minimums?
     */
    public static boolean versionMeetsMinimum(int major, int minor, int subminor, MysqlVersion mysqlVersion) {
        if (mysqlVersion.getServerMajorVersion() >= major) {
            if (mysqlVersion.getServerMajorVersion() == major) {
                if (mysqlVersion.getServerMinorVersion() >= minor) {
                    if (mysqlVersion.getServerMinorVersion() == minor) {
                        return mysqlVersion.getServerSubMinorVersion() >= subminor;
                    }
                    // newer than major.minor
                    return true;
                }
                // older than major.minor
                return false;
            }
            // newer than major
            return true;
        }
        return false;
    }

    public static MysqlVersion parseVersion(String version) {
        // Parse the server version into major/minor/subminor
        int point = version.indexOf('.');

        MysqlVersion mysqlVersion = new MysqlVersion();
        if (point != -1) {
            try {
                mysqlVersion.setServerMajorVersion(Integer.parseInt(version.substring(0, point)));
            } catch (NumberFormatException e) {
                // ignore
                LOGGER.warn("version[{}] format is wrong", version);
            }

            String remaining = version.substring(point + 1);
            point = remaining.indexOf('.');

            if (point != -1) {
                try {
                    mysqlVersion.setServerMinorVersion(Integer.parseInt(remaining.substring(0, point)));
                } catch (NumberFormatException e) {
                    // ignore
                    LOGGER.warn("version[{}] format is wrong", version);
                }

                remaining = remaining.substring(point + 1);

                int pos = 0;

                while (pos < remaining.length()) {
                    if ((remaining.charAt(pos) < '0') || (remaining.charAt(pos) > '9')) {
                        break;
                    }
                    pos++;
                }

                try {
                    mysqlVersion.setServerSubMinorVersion(Integer.parseInt(remaining.substring(0, pos)));
                } catch (NumberFormatException e) {
                    // ignore
                    LOGGER.warn("version[{}] format is wrong", version);
                }
            }
        }
        return mysqlVersion;
    }


}
