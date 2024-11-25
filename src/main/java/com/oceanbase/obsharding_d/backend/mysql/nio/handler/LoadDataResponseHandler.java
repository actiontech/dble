/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler;


import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

import javax.annotation.Nonnull;

/**
 * Created by nange on 2015/3/31.
 */
public interface LoadDataResponseHandler {
    void requestDataResponse(byte[] row, @Nonnull MySQLResponseService service);
}
