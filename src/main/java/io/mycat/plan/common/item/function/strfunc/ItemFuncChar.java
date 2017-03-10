package io.mycat.plan.common.item.function.strfunc;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemFuncChar extends ItemStrFunc {
	private String mysqlCharset;
	private String javaCharset;

	public ItemFuncChar(List<Item> args) {
		this(args, null);
	}

	public ItemFuncChar(List<Item> args, String charset) {
		super(args);
		this.mysqlCharset = charset;
	}

	@Override
	public final String funcName() {
		return "CHAR";
	}

	@Override
	public void fixLengthAndDec() {
		javaCharset = mysqlCharset == null ? null : CharsetUtil.getJavaCharset(mysqlCharset);
		maxLength = args.size() * 4;
	}

	@Override
	public String valStr() {
		byte[] b = new byte[args.size()];
		int count = 0;
		for (Item arg : args) {
			if (!arg.isNull()) {
				byte c = (byte) arg.valInt().intValue();
				b[count++] = c;
			}
		}
		b[count++] = 0;
		try {
			if (javaCharset == null)
				return new String(b);
			else
				return new String(b, javaCharset);
		} catch (UnsupportedEncodingException e) {
			nullValue = true;
			logger.warn("char() charset exception:", e);
			return null;
		}
	}

	@Override
	public SQLExpr toExpression() {
		SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
		for (Item arg : args) {
			method.addParameter(arg.toExpression());
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
		return new ItemFuncChar(newArgs, mysqlCharset);
	}

}
