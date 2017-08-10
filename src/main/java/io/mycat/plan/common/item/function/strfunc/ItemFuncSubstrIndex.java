package io.mycat.plan.common.item.function.strfunc;

import java.util.ArrayList;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

public class ItemFuncSubstrIndex extends ItemStrFunc {

	public ItemFuncSubstrIndex(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "SUBSTRING_INDEX";
	}

	@Override
	public String valStr() {
		String str = args.get(0).valStr();
		String delim = args.get(1).valStr();
		int count = args.get(2).valInt().intValue();
		if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull() || args.get(2).isNull()))
			return EMPTY;
		if (str.length() == 0 || delim.length() == 0 || count == 0)
			return EMPTY;

		int delimLen = delim.length();

		List<String> subs = new ArrayList<String>();
		while (true) {
			int index = str.indexOf(delim);
			if (index < 0) {
				subs.add(str);
				break;
			}
			if (index == 0) {
				str = str.substring(index + delimLen);
			} else {
				subs.add(str.substring(0, index));
				str = str.substring(index + delimLen);
			}
		}
		StringBuilder ret = new StringBuilder();
		if (count > 0) {
			int trueCount = Math.min(subs.size(), count);
			for (int i = 0; i < trueCount; i++) {
				ret.append(subs.get(i) + delim);
			}
		} else {
			int trueCount = Math.min(subs.size(), -count);
			for (int i = subs.size() - trueCount; i < subs.size(); i++){
				ret.append(subs.get(i) + delim);
			}
		}
		// 得删除最后一个delim
		int lastDelim = ret.lastIndexOf(delim);
		return ret.substring(0, lastDelim);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncSubstrIndex(realArgs);
	}
}
