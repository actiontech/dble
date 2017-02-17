package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.BoolPtr;
import io.mycat.plan.common.time.MySQLTimestampType;

public class GetDatetimeValue implements GetValueFunc {

	@Override
	public long get(Item item, Item warn_item, BoolPtr is_null) {
		long value = 0;
		String str = null;
		if (item.isTemporal()) {
			value = item.valDateTemporal();
			is_null.set(item.nullValue);
		} else {
			str = item.valStr();
			is_null.set(item.nullValue);
		}
		if (is_null.get())
			return 0;
		if (str != null) {
			BoolPtr error = new BoolPtr(false);
			FieldTypes f_type = warn_item.fieldType();
			MySQLTimestampType t_type = f_type == FieldTypes.MYSQL_TYPE_DATE
					? MySQLTimestampType.MYSQL_TIMESTAMP_DATE
					: MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
			value = MySQLcom.get_date_from_str(str, t_type, error);
		}
		return value;
	}

}
