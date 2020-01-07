/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql;

/**
 * Created by szf on 2018/12/7.
 */
public class CollationInfo {
    private final String collation;
    private final String charset;
    private final int id;
    private final boolean isDefault;

    CollationInfo(String collation, String charset, int id, boolean isDefault) {
        this.collation = collation;
        this.charset = charset;
        this.id = id;
        this.isDefault = isDefault;
    }
    public String getCollation() {
        return collation;
    }

    public String getCharset() {
        return charset;
    }

    public int getId() {
        return id;
    }

    public boolean isDefault() {
        return isDefault;
    }

}
