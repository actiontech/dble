package io.mycat.plan.common.item.function.castfunc;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;


public class ItemCharTypecast extends ItemStrFunc {
	private int cast_length;
	private String charSetName;
	public ItemCharTypecast(Item a, int length_arg, String charSetName) {
		super(new ArrayList<Item>());
		args.add(a);
		this.cast_length = length_arg;
		this.charSetName = charSetName;
	}

	@Override
	public final String funcName() {
		return "cast_as_char";
	}

	@Override
	public void fixLengthAndDec() {
		fixCharLength(cast_length >= 0 ? cast_length : args.get(0).maxLength);
	}

	@Override
	public String valStr() {
		assert (fixed == true && cast_length >= 0);

		String res = null;
		if ((res = args.get(0).valStr()) == null) {
			nullValue = true;
			return null;
		}
		nullValue = false;
		if (cast_length < res.length()) {
			res = res.substring(0, cast_length);
		}
		if(charSetName != null){
			try {
				res = new String(res.getBytes(),CharsetUtil.getJavaCharset(charSetName));
			} catch (UnsupportedEncodingException e) {
				logger.warn("convert using charset exception", e);
				nullValue = true;
				return null;
			}
		}
		return res;
	}

	@Override
	public SQLExpr toExpression() {
		SQLCastExpr cast = new SQLCastExpr();
		cast.setExpr(args.get(0).toExpression());
		SQLCharacterDataType dataType = new SQLCharacterDataType(SQLCharacterDataType.CHAR_TYPE_CHAR);
		cast.setDataType(dataType);
		if (cast_length >= 0) {
			dataType.addArgument(new SQLIntegerExpr(cast_length));
		}
		if (charSetName != null) {
			dataType.setName(charSetName);
		}
		cast.setDataType(dataType);
		return cast;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemCharTypecast(newArgs.get(0), cast_length, charSetName);
	}
}
