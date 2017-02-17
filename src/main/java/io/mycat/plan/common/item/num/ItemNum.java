package io.mycat.plan.common.item.num;

import io.mycat.plan.common.item.ItemBasicConstant;

public abstract class ItemNum extends ItemBasicConstant {

	public ItemNum() {
		// my_charset_numeric
		charsetIndex = 8;
	}

	public abstract ItemNum neg();
}
