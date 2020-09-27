/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

/**
 * isolation level
 *
 * @author mycat
 */
public final class Isolations {
    private Isolations() {
    }

    public static final int READ_UNCOMMITTED = 1;
    public static final int READ_COMMITTED = 2;
    public static final int REPEATABLE_READ = 3;
    public static final int SERIALIZABLE = 4;

    public static String getIsolation(int i) {
        switch (i) {
            case READ_UNCOMMITTED:
                return "read-uncommitted";
            case READ_COMMITTED:
                return "read-committed";
            case SERIALIZABLE:
                return "serializable";
            case REPEATABLE_READ:
            default:
                return "repeatable-read";
        }
    }

}
