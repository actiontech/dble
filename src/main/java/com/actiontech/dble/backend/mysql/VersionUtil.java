/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql;

public final class VersionUtil {
    private VersionUtil() {
    }

    public static final String TX_READ_ONLY = "tx_read_only";
    public static final String TRANSACTION_READ_ONLY = "transaction_read_only";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";

    public static String getIsolationNameByVersion(String version) {
        if (version == null) {
            return null;
        } else if (version.startsWith("8")) {
            return TRANSACTION_ISOLATION;
        } else {
            return TX_ISOLATION;
        }
    }
}
