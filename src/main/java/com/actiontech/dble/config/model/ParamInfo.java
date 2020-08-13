/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model;

public class ParamInfo {
    private final String name;
    private final String value;
    private final String comment;

    public ParamInfo(String name, String value, String comment) {
        this.name = name;
        this.value = value;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public String getComment() {
        return comment;
    }

}
