/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

/**
 * Created by szf on 2019/10/29.
 */
public class DbInstanceStatus {
    private String name;
    private boolean disable;
    private boolean primary;

    public DbInstanceStatus(String name, boolean disable, boolean primary) {
        this.name = name;
        this.disable = disable;
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public boolean isDisable() {
        return disable;
    }

    public boolean isPrimary() {
        return primary;
    }
}
