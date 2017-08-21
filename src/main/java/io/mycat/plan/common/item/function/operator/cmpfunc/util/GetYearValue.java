package io.mycat.plan.common.item.function.operator.cmpfunc.util;


import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.ptr.BoolPtr;
import io.mycat.plan.common.time.MyTime;

/*
 Retrieves YEAR value of 19XX-00-00 00:00:00 form from given item.

 SYNOPSIS
 get_year_value()
 thd                 thread handle
 item_arg   [in/out] item to retrieve YEAR value from
 cache_arg  [in/out] pointer to place to store the caching item to
 warn_item  [in]     item for issuing the conversion warning
 is_null    [out]    TRUE <=> the item_arg is null

 DESCRIPTION
 Retrieves the YEAR value of 19XX form from given item for comparison by the
 compare_datetime() function.
 Converts year to DATETIME of form YYYY-00-00 00:00:00 for the compatibility
 with the get_datetime_value function result.

 RETURN
 obtained value
 */
public class GetYearValue implements GetValueFunc {

    @Override
    public long get(Item item, Item warnitem, BoolPtr is_null) {
        long value = 0;

        value = item.valInt().longValue();
        is_null.set(item.nullValue);
        if (is_null.get())
            return 0;

		/*
         * Coerce value to the 19XX form in order to correctly compare YEAR(2) &
		 * YEAR(4) types. Here we are converting all item values but YEAR(4)
		 * fields since 1) YEAR(4) already has a regular YYYY form and 2) we
		 * don't want to convert zero/bad YEAR(4) values to the value of 2000.
		 */
        if (item.type() == ItemType.FIELD_ITEM) {
            Field field = ((ItemField) item).field;
            if (field.fieldType() == FieldTypes.MYSQL_TYPE_YEAR && field.fieldLength == 4) {
                if (value < 70)
                    value += 100;
                if (value <= 1900)
                    value += 1900;
            }
        }
        /* Convert year to DATETIME packed format */
        return MyTime.year_to_longlong_datetime_packed(value);
    }
}
