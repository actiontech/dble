/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

import java.util.List;

/**
 * Created by szf on 2019/5/30.
 */
public interface InnerFuncResponse {

    List<FieldPacket> getField();

    List<RowDataPacket> getRows(ShardingService service);

}
