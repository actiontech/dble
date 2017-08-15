package io.mycat.plan.common.item.function.strfunc;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemFuncChar extends ItemStrFunc {
	private String mysqlCharset;

	public ItemFuncChar(List<Item> args,int charsetIndex) {
		super(args);
		this.mysqlCharset = CharsetUtil.getCharset(charsetIndex);
		this.charsetIndex = charsetIndex;
	}

	public ItemFuncChar(List<Item> args, String charset) {
		super(args);
		this.mysqlCharset = charset;
		this.charsetIndex = CharsetUtil.getIndex(charset);
	}

	@Override
	public final String funcName() {
		return "CHAR";
	}

	@Override
	public void fixLengthAndDec() {
		maxLength = args.size() * 4;
	}

	@Override
	public String valStr() {
		List<Byte> bytes = new ArrayList<>(args.size());
		for (Item arg : args) {
			if (!arg.isNull()) {
				int value = arg.valInt().intValue();
				String hex = Integer.toHexString(value);
				if (hex.length() % 2 != 0) {
					hex = "0" + hex;
				}
				for (int i = 0; i < hex.length(); i = i + 2) {
					bytes.add((byte)(Integer.parseInt(hex.substring(i, i + 2), 16)));
				}
			}
		}
		byte[] b = new byte[bytes.size()];
		for (int i = 0; i < bytes.size(); i++) {
			b[i] = bytes.get(i);
		}
		try {
			return new String(b, CharsetUtil.getJavaCharset(mysqlCharset));
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
