package io.mycat.plan.common.item.function.strfunc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

import io.mycat.plan.common.item.Item;

public class ItemFuncFormat extends ItemStrFunc {

	public ItemFuncFormat(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "FORMAT";
	}

	@Override
	public String valStr() {
		BigDecimal bd = args.get(0).valDecimal();
		int pl = args.get(1).valInt().intValue();
		if (pl < 0)
			pl = 0;
		String local = "en_US";
		if (args.size() == 3)
			local = args.get(2).valStr();
		Locale loc = new Locale(local);
		NumberFormat f = DecimalFormat.getInstance(loc);
		if (args.get(0).isNull() || args.get(1).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		BigDecimal bdnew = bd.setScale(pl, RoundingMode.HALF_UP);
		return f.format(bdnew);
	}

}
