/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

/**
 * @author dcy
 * Create Date: 2021-05-11
 */
public abstract class InnerServiceTask extends ServiceTask {
    public InnerServiceTask(Service service) {
        super(service);
    }
}
