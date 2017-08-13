package io.mycat.plan.common.item.function.castfunc;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.timefunc.ItemTimeFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

public class ItemTimeTypecast extends ItemTimeFunc {

	public ItemTimeTypecast(Item a) {
		super(new ArrayList<Item>());
		args.add(a);
	}

	public ItemTimeTypecast(Item a, int dec_arg) {
		super(new ArrayList<Item>());
		args.add(a);
		decimals = dec_arg;
	}

	@Override
	public final String funcName() {
		return "cast_as_time";
	}

	public boolean getTime(MySQLTime ltime) {
		if (getArg0Time(ltime))
			return true;
		if (decimals != NOT_FIXED_DEC) {
			MyTime.my_time_round(ltime, decimals);
		}
		/*
		 * For MYSQL_TIMESTAMP_TIME value we can have non-zero day part, which
		 * we should not lose.
		 */
		if (ltime.time_type != MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
			MyTime.datetime_to_time(ltime);
		return false;
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = true;
	}

	@Override
	public SQLExpr toExpression() {
		SQLCastExpr cast = new SQLCastExpr();
		cast.setExpr(args.get(0).toExpression());
		SQLDataTypeImpl dataType = new SQLDataTypeImpl("TIME");
		if (decimals != NOT_FIXED_DEC) {
			dataType.addArgument(new SQLIntegerExpr(decimals));
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
		return new ItemTimeTypecast(newArgs.get(0), this.decimals);
	}
}
