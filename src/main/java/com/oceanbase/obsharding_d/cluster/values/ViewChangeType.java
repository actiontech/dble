/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public class ViewChangeType {
    String type;

    String instanceName;

    public ViewChangeType() {
    }

    public ViewChangeType(String instanceName, String type) {
        this.type = type;
        this.instanceName = instanceName;
    }

    public String getType() {
        return type;
    }

    public String getInstanceName() {
        return instanceName;

    }

    @Override
    public String toString() {
        return "ViewChangeType{" +
                "type='" + type + '\'' +
                ", instanceName='" + instanceName + '\'' +
                '}';
    }
}
