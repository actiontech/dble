/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;


import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.List;

/**
 * Created by nange on 2015/3/31.
 */
public interface PreparedResponseHandler {

    void preparedOkResponse(byte[] ok, List<byte[]> fields, List<byte[]> params, MySQLResponseService service);

    void preparedExecuteResponse(byte[] header, List<byte[]> fields, byte[] eof, MySQLResponseService service);
}
