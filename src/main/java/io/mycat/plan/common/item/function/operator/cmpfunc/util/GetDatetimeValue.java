package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.BoolPtr;
import io.mycat.plan.common.time.MySQLTimestampType;

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
