/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;

import java.util.List;

/**
 * timezone change,not support
 */
public class ItemFuncConvTz extends ItemDatetimeFunc {

    public ItemFuncConvTz(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "convert_tz";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unsupported function convert_tz!");
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConvTz(realArgs, charsetIndex);
    }

}
