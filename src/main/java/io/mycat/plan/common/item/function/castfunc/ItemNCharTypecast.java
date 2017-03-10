package io.mycat.plan.common.item.function.castfunc;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;


public class ItemNCharTypecast extends ItemStrFunc {
	private int cast_length;
	public ItemNCharTypecast(Item a, int length_arg) {
		super(new ArrayList<Item>());
		args.add(a);
		this.cast_length = length_arg;
	}

	@Override
	public final String funcName() {
		return "cast_as_nchar";
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
		if (cast_length < res.length())
			res = res.substring(0, cast_length);
		return res;
	}

	@Override
	public SQLExpr toExpression() {
		SQLCastExpr cast = new SQLCastExpr();
		cast.setExpr(args.get(0).toExpression());
		SQLDataTypeImpl dataType = new SQLDataTypeImpl("NCAHR"); 
		if(cast_length >=0){
			dataType.addArgument(new SQLIntegerExpr(cast_length));
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
		return new ItemNCharTypecast(newArgs.get(0), cast_length);
	}
}
