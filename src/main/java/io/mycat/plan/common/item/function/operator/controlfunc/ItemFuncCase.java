package io.mycat.plan.common.item.function.operator.controlfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import io.mycat.plan.common.time.MySQLTime;


public class ItemFuncCase extends ItemFunc {

	int first_expr_num, else_expr_num;
	ItemResult cached_result_type, left_result_type;
	int ncases;
	ItemResult cmp_type;
	FieldTypes cached_field_type;

	/**
	 * @param args
	 * @param first_expr_num
	 *            -1 代表没有case表达式,否则代表case表达式在args中的index，case和else exp在队列的最后
	 * @param else_expr_num
	 *            else在args中的index
	 */
	public ItemFuncCase(List<Item> args, int ncases, int first_expr_num, int else_expr_num) {
		super(args);
		this.ncases = ncases;
		this.first_expr_num = first_expr_num;
		this.else_expr_num = else_expr_num;
		this.cached_result_type = ItemResult.INT_RESULT;
		this.left_result_type = ItemResult.INT_RESULT;
	}

	@Override
	public final String funcName() {
		return "case";
	}

	@Override
	public void fixLengthAndDec() {
		List<Item> agg = new ArrayList<Item>();
		int nagg;
		/*
		 * Aggregate all THEN and ELSE expression types and collations when
		 * string result
		 */

		for (nagg = 0; nagg < ncases / 2; nagg++)
			agg.add(args.get(nagg * 2 + 1));
		if (else_expr_num != -1)
			agg.add(args.get(else_expr_num));
		cached_field_type = MySQLcom.agg_field_type(agg, 0, agg.size());
		cached_result_type = MySQLcom.agg_result_type(agg, 0, agg.size());
		if (first_expr_num != -1)
			left_result_type = args.get(first_expr_num).resultType();
	}

	@Override
	public ItemResult resultType() {
		return cached_result_type;
	}

	@Override
	public FieldTypes fieldType() {
		return cached_field_type;
	}

	@Override
	public BigDecimal valReal() {
		Item item = findItem();
		if (item == null) {
			nullValue = true;
			return BigDecimal.ZERO;
		}
		BigDecimal res = item.valReal();
		nullValue = item.nullValue;
		return res;
	}

	@Override
	public BigInteger valInt() {
		Item item = findItem();
		if (item == null) {
			nullValue = true;
			return BigInteger.ZERO;
		}
		BigInteger res = item.valInt();
		nullValue = item.nullValue;
		return res;
	}

	@Override
	public String valStr() {
		FieldTypes i = fieldType();
		if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
			return valStringFromDatetime();
		} else if (i == FieldTypes.MYSQL_TYPE_DATE) {
			return valStringFromDate();
		} else if (i == FieldTypes.MYSQL_TYPE_TIME) {
			return valStringFromTime();
		} else {
			Item item = findItem();
			if (item != null) {
				String res;
				if ((res = item.valStr()) != null) {
					nullValue = false;
					return res;
				}
			}
		}
		nullValue = true;
		return null;
	}

	@Override
	public BigDecimal valDecimal() {
		Item item = findItem();
		if (item == null) {
			nullValue = true;
			return null;
		}
		BigDecimal res = item.valDecimal();
		nullValue = item.nullValue;
		return res;
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		Item item = findItem();
		if (item == null)
			return (nullValue = true);
		return (nullValue = item.getDate(ltime, fuzzydate));
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		Item item = findItem();
		if (item == null)
			return (nullValue = true);
		return (nullValue = item.getTime(ltime));
	}

	/**
	 * Find and return matching items for CASE or ELSE item if all compares are
	 * failed or NULL if ELSE item isn't defined.
	 * 
	 * IMPLEMENTATION In order to do correct comparisons of the CASE expression
	 * (the expression between CASE and the first WHEN) with each WHEN
	 * expression several comparators are used. One for each result type. CASE
	 * expression can be evaluated up to # of different result types are used.
	 * To check whether the CASE expression already was evaluated for a
	 * particular result type a bit mapped variable value_added_map is used.
	 * Result types are mapped to it according to their int values i.e.
	 * STRING_RESULT is mapped to bit 0, REAL_RESULT to bit 1, so on.
	 * 
	 * @retval NULL Nothing found and there is no ELSE expression defined
	 * @retval item Found item or ELSE item if defined and all comparisons are
	 *         failed
	 */
	private Item findItem() {
		if (first_expr_num == -1) {
			for (int i = 0; i < ncases; i += 2) {
				// No expression between CASE and the first WHEN
				if (args.get(i).valBool())
					return args.get(i + 1);
				continue;
			}
		} else {
			/* Compare every WHEN argument with it and return the first match */
			Item leftCmpItem = args.get(first_expr_num);
			if (leftCmpItem.isNull() || leftCmpItem.type() == ItemType.NULL_ITEM) {
				return else_expr_num != -1 ? args.get(else_expr_num) : null;
			}
			for (int i = 0; i < ncases; i += 2) {
				if (args.get(i).type() == ItemType.NULL_ITEM)
					continue;
				Item rightCmpItem = args.get(i);
				ArgComparator cmptor = new ArgComparator(leftCmpItem, rightCmpItem);
				cmptor.setCmpFunc(null, leftCmpItem, rightCmpItem, false);
				if (cmptor.compare() == 0 && !rightCmpItem.nullValue)
					return args.get(i + 1);
			}
		}
		// No, WHEN clauses all missed, return ELSE expression
		return else_expr_num != -1 ? args.get(else_expr_num) : null;
	}

	// @Override
	// protected Item cloneStruct() {
	// List<Item> newArgList = cloneStructList(args);
	// return new Item_func_case(newArgList, ncases, first_expr_num,
	// else_expr_num);
	// }

	@Override
	public SQLExpr toExpression() {
		SQLCaseExpr caseExpr = new SQLCaseExpr();
		List<SQLExpr> exprList = toExpressionList(args);
		for (int index = 0; index < ncases;) {
			SQLExpr exprCond = exprList.get(index++);
			SQLExpr exprValue = exprList.get(index++);

			SQLCaseExpr.Item item = new SQLCaseExpr.Item(exprCond,exprValue);
			caseExpr.addItem(item);
		}
		if (first_expr_num > 0) {
			caseExpr.setValueExpr(exprList.get(first_expr_num));
		}
		if (else_expr_num > 0) {
			caseExpr.setElseExpr(exprList.get(else_expr_num));
		}
		return caseExpr;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemFuncCase(newArgs, ncases, first_expr_num, else_expr_num);
	}
}
