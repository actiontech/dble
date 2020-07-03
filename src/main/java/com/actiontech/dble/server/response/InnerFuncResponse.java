package com.actiontech.dble.server.response;

import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.util.List;

/**
 * Created by szf on 2019/5/30.
 */
public interface InnerFuncResponse {

    List<FieldPacket> getField();

    List<RowDataPacket> getRows(ShardingService service);

}
