package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.ItemFuncKeyWord;


public class ItemFuncTrim extends ItemStrFunc {
	public enum TRIM_TYPE_ENUM{
		DEFAULT,BOTH,LEADING,TRAILING,LTRIM,RTRIM
	}
	private TRIM_TYPE_ENUM mTrimMode;
	private boolean mTrimLeading;
	private boolean mTrimTrailing;

	public ItemFuncTrim(Item a, Item b, TRIM_TYPE_ENUM tm) {
		super(a, b);
		this.mTrimMode = tm;
		mTrimLeading = trimLeading();
		mTrimTrailing = trimTrailing();
	}

	public ItemFuncTrim(Item a, TRIM_TYPE_ENUM tm) {
		super(a);
		this.mTrimMode = tm;
		mTrimLeading = trimLeading();
		mTrimTrailing = trimTrailing();
	}

	private final boolean trimLeading() {
		return mTrimMode == TRIM_TYPE_ENUM.DEFAULT || mTrimMode == TRIM_TYPE_ENUM.BOTH || mTrimMode == TRIM_TYPE_ENUM.LEADING
				|| mTrimMode == TRIM_TYPE_ENUM.LTRIM;
	}

	private final boolean trimTrailing() {
		return mTrimMode == TRIM_TYPE_ENUM.DEFAULT || mTrimMode == TRIM_TYPE_ENUM.BOTH || mTrimMode == TRIM_TYPE_ENUM.TRAILING
				|| mTrimMode == TRIM_TYPE_ENUM.RTRIM;
	}

	@Override
	public final String funcName() {
		switch (mTrimMode) {
		case DEFAULT:
			return "trim";
		case BOTH:
			return "trim";
		case LEADING:
			return "ltrim";
		case TRAILING:
			return "rtrim";
		case LTRIM:
			return "ltrim";
		case RTRIM:
			return "rtrim";
		}
		return null;
	}

	@Override
	public void fixLengthAndDec() {

	}

	@Override
	public String valStr() {
		String toTrim = args.get(0).valStr();
		if (nullValue = args.get(0).nullValue)
			return null;
		String remove = null;
		if (getArgCount() == 2) {
			remove = args.get(1).valStr();
			if (nullValue = args.get(1).nullValue)
				return null;
		}
		String ret = null;
		if (mTrimLeading)
			ret = StringUtils.stripStart(toTrim, remove);
		if (mTrimTrailing)
			ret = StringUtils.stripEnd(toTrim, remove);
		return ret;
	}

	@Override
	public SQLExpr toExpression() {
		SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
		switch (mTrimMode) {
		case LTRIM:
			method.setMethodName("LTRIM");
			method.addParameter(args.get(0).toExpression());
			break;
		case RTRIM:
			method.setMethodName("RTRIM");
			method.addParameter(args.get(0).toExpression());
			break;
		default:
			method.setMethodName("TRIM");
			method.addParameter(args.get(0).toExpression());
			if (this.getArgCount() > 1){
				method.putAttribute(ItemFuncKeyWord.FROM, args.get(1).toExpression());
			}
			if(mTrimMode!=TRIM_TYPE_ENUM.DEFAULT){
				method.putAttribute(ItemFuncKeyWord.TRIM_TYPE, mTrimMode.toString());
			}
		}
		return method;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		if (getArgCount() == 2)
			return new ItemFuncTrim(newArgs.get(0), newArgs.get(1), mTrimMode);
		else
			return new ItemFuncTrim(newArgs.get(0), mTrimMode);
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		if (getArgCount() == 2)
			return new ItemFuncTrim(realArgs.get(0), realArgs.get(1), mTrimMode);
		else
			return new ItemFuncTrim(realArgs.get(0), mTrimMode);
	}

}
