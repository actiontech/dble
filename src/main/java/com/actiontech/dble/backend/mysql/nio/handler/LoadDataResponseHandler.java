/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;


import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import javax.annotation.Nonnull;

/**
 * Created by nange on 2015/3/31.
 */
public interface LoadDataResponseHandler {
    void requestDataResponse(byte[] row, @Nonnull MySQLResponseService service);
}
