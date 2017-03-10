/**
 * 
 */
package io.mycat.plan.common.item.function.mathsfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;


public class ItemFuncMd5 extends ItemStrFunc {

	/**
	 * @param name
	 * @param a
	 */
	public ItemFuncMd5(Item a) {
		super(a);
	}

	@Override
	public final String funcName() {
		return "md5";
	}

	@Override
	public String valStr() {
		String value = args.get(0).valStr();
		if (value != null) {
			nullValue = false;

		}
		// TODO
		return null;
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncMd5(realArgs.get(0));
	}
}
