/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function;

import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;
import com.oceanbase.obsharding_d.server.response.InnerFuncResponse;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Created by szf on 2019/5/28.
 */
public class ItemFuncInner extends ItemFunc {

    private final InnerFuncResponse rspHandler;
    private final String funcName;

    public ItemFuncInner(String funcName, List<Item> args, InnerFuncResponse rspHandler, int charsetIndex) {
        super(args, charsetIndex);
        this.funcName = funcName;
        this.rspHandler = rspHandler;
    }

    public List<FieldPacket> getField() {
        return rspHandler.getField();
    }

    public List<RowDataPacket> getRows(ShardingService service) {
        return rspHandler.getRows(service);
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public String funcName() {
        return funcName;
    }

    @Override
    public BigDecimal valReal() {
        return null;
    }

    @Override
    public BigInteger valInt() {
        return null;
    }

    @Override
    public String valStr() {
        return null;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return false;
    }
}
