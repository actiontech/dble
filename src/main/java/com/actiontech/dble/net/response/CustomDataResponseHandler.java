/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.response;

import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

/**
 * use it when need override data func
 *
 * @author dcy
 * Create Date: 2022-08-18
 */
public abstract class CustomDataResponseHandler extends DefaultResponseHandler {
    public CustomDataResponseHandler(MySQLResponseService service) {
        super(service);
    }


    @Override
    protected void beforeError() {
        //doesn't setRowDataFlowing. just do nothing.
    }


    @Override
    public abstract void data(byte[] data);
}
