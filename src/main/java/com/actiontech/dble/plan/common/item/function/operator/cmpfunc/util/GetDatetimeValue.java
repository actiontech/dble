/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;

public class GetDatetimeValue implements GetValueFunc {

    @Override
    public long get(Item item, Item warnItem, BoolPtr isNull) {
        long value = 0;
        String str = null;
        if (item.isTemporal()) {
            value = item.valDateTemporal();
            isNull.set(item.isNullValue());
        } else {
            str = item.valStr();
            isNull.set(item.isNullValue());
        }
        if (isNull.get())
            return 0;
        if (str != null) {
            BoolPtr error = new BoolPtr(false);
            FieldTypes fType = warnItem.fieldType();
            MySQLTimestampType tType = fType == FieldTypes.MYSQL_TYPE_DATE ?
                    MySQLTimestampType.MYSQL_TIMESTAMP_DATE :
                    MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
            value = MySQLcom.getDateFromStr(str, tType, error);
        }
        return value;
    }

}
