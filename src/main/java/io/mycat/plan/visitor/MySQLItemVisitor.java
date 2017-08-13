package io.mycat.plan.visitor;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.config.ErrorCode;
import io.mycat.plan.Order;
import io.mycat.plan.common.CastTarget;
import io.mycat.plan.common.CastType;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.ItemFloat;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.ItemNull;
import io.mycat.plan.common.item.ItemString;
import io.mycat.plan.common.item.function.ItemCreate;
import io.mycat.plan.common.item.function.ItemFuncKeyWord;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncBitAnd;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncBitInversion;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncBitOr;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncBitXor;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncLeftShift;
import io.mycat.plan.common.item.function.bitfunc.ItemFuncRightShift;
import io.mycat.plan.common.item.function.castfunc.ItemCharTypecast;
import io.mycat.plan.common.item.function.castfunc.ItemFuncBinary;
import io.mycat.plan.common.item.function.castfunc.ItemFuncConvCharset;
import io.mycat.plan.common.item.function.castfunc.ItemNCharTypecast;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncDiv;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncIntDiv;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncMinus;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncMod;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncMul;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncNeg;
import io.mycat.plan.common.item.function.mathsfunc.operator.ItemFuncPlus;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncBetweenAnd;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGt;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIsfalse;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIsnotfalse;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIsnotnull;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIsnottrue;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIsnull;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIstrue;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLike;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLt;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncNe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncRegex;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncStrictEqual;
import io.mycat.plan.common.item.function.operator.controlfunc.ItemFuncCase;
import io.mycat.plan.common.item.function.operator.controlfunc.ItemFuncIf;
import io.mycat.plan.common.item.function.operator.logic.ItemCondAnd;
import io.mycat.plan.common.item.function.operator.logic.ItemCondOr;
import io.mycat.plan.common.item.function.operator.logic.ItemFuncNot;
import io.mycat.plan.common.item.function.operator.logic.ItemFuncXor;
import io.mycat.plan.common.item.function.strfunc.ItemFuncChar;
import io.mycat.plan.common.item.function.strfunc.ItemFuncOrd;
import io.mycat.plan.common.item.function.strfunc.ItemFuncTrim;
import io.mycat.plan.common.item.function.strfunc.ItemFuncTrim.TRIM_TYPE_ENUM;
import io.mycat.plan.common.item.function.sumfunc.ItemFuncGroupConcat;
import io.mycat.plan.common.item.function.sumfunc.ItemSumAnd;
import io.mycat.plan.common.item.function.sumfunc.ItemSumAvg;
import io.mycat.plan.common.item.function.sumfunc.ItemSumCount;
import io.mycat.plan.common.item.function.sumfunc.ItemSumMax;
import io.mycat.plan.common.item.function.sumfunc.ItemSumMin;
import io.mycat.plan.common.item.function.sumfunc.ItemSumOr;
import io.mycat.plan.common.item.function.sumfunc.ItemSumStd;
import io.mycat.plan.common.item.function.sumfunc.ItemSumSum;
import io.mycat.plan.common.item.function.sumfunc.ItemSumVariance;
import io.mycat.plan.common.item.function.sumfunc.ItemSumXor;
import io.mycat.plan.common.item.function.timefunc.ItemDateAddInterval;
import io.mycat.plan.common.item.function.timefunc.ItemExtract;
import io.mycat.plan.common.item.function.timefunc.ItemFuncGetFormat;
import io.mycat.plan.common.item.function.timefunc.ItemFuncTimestampDiff;
import io.mycat.plan.common.item.function.unknown.ItemFuncUnknown;
import io.mycat.plan.common.item.subquery.ItemInSubselect;
import io.mycat.plan.common.item.subquery.ItemSinglerowSubselect;
import io.mycat.util.StringUtil;

public class MySQLItemVisitor extends MySqlASTVisitorAdapter {
	private String currentDb;
	private int charsetIndex;

	public MySQLItemVisitor(String currentDb) {
		this(currentDb, CharsetUtil.getIndex("utf8"));
	}
	public MySQLItemVisitor(String currentDb, int charsetIndex) {
		this.currentDb = currentDb;
		this.charsetIndex = charsetIndex;
	}
	private Item item;

	public Item getItem() {
		return item;
	}
	public void setItem(Item item) {
		this.item = item;
	}
	@Override
	public void endVisit(SQLQueryExpr x) {
		SQLSelectQuery sqlSelect = x.getSubQuery().getQuery();
		item = new ItemSinglerowSubselect(currentDb, sqlSelect);
    }

	@Override
	public void endVisit(SQLBetweenExpr x){
		item = new ItemFuncBetweenAnd(getItem(x.getTestExpr()), getItem(x.getBeginExpr()), getItem(x.getEndExpr()), x.isNot());
		initName(x);
	}

	@Override
	public void endVisit(SQLInSubQueryExpr x){
		boolean isNeg = x.isNot();
		Item left = getItem(x.getExpr());
		item = new ItemInSubselect(currentDb, left, x.getSubQuery().getQuery(), isNeg);
		initName(x);
	}

	@Override
	public void endVisit(SQLBooleanExpr x) {
		if(x.getValue()){
			item = new ItemInt(1);
		}else{
			item = new ItemInt(0);
		}
		initName(x);
	}

	@Override
	public void endVisit(SQLBinaryOpExpr x){
		Item itemLeft = getItem(x.getLeft());
		Item itemRight = getItem(x.getRight());
		switch (x.getOperator()) {
		case Is:
			// is null, or is unknown
			if (itemRight instanceof ItemNull || itemRight instanceof ItemString) {
				item = new ItemFuncIsnull(itemLeft);
			} else if (itemRight instanceof ItemInt) {
				ItemInt itemBool = (ItemInt) itemRight;
				if (itemBool.valInt().longValue() == 1) {// is true
					item = new ItemFuncIstrue(itemLeft);
				} else {
					item = new ItemFuncIsfalse(itemLeft);
				}
			} else{
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support type:" + x.getRight());
			}
			break;
		case IsNot:
			// is not null, or is notunknown
			if (itemRight instanceof ItemNull || itemRight instanceof ItemString) {
				item = new ItemFuncIsnotnull(itemLeft);
			} else if (itemRight instanceof ItemInt) {
				ItemInt itemBool = (ItemInt) itemRight;
				if (itemBool.valInt().longValue() == 1) {// is true
					item = new ItemFuncIsnottrue(itemLeft);
				} else {
					item = new ItemFuncIsnotfalse(itemLeft);
				}
			} else{
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support type:" + x.getRight());
			}
			break;
		case Escape:
			if (itemLeft instanceof ItemFuncLike) {
				// A LIKE B ESCAPE C ,A is "itemLeft"
				SQLBinaryOpExpr like = (SQLBinaryOpExpr) (x.getLeft());
				Item itemLikeLeft = getItem(like.getLeft());
				Item itemLikeRight = getItem(x.getRight());
				boolean isNot = (like.getOperator() == SQLBinaryOperator.NotLike);
				item = new ItemFuncLike(itemLikeLeft, itemLikeRight, itemRight, isNot);
			} else {
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
						"not supported kind expression:" + x.getOperator());
			}
			break;
		case NotLike:
			item = new ItemFuncLike(itemLeft, itemRight, null, true);
			break;
		case Like:
			item = new ItemFuncLike(itemLeft, itemRight, null, false);
			break;
		case Equality:
			item = new ItemFuncEqual(itemLeft, itemRight);
			break;
		case Add:
			item = new ItemFuncPlus(itemLeft, itemRight);
			break;
		case Divide:
			item = new ItemFuncDiv(itemLeft, itemRight);
			break;
		case DIV:
			item = new ItemFuncIntDiv(itemLeft, itemRight);
			break;
		case Mod:
		case Modulus:
			item = new ItemFuncMod(itemLeft, itemRight);
			break;
		case Multiply:
			item = new ItemFuncMul(itemLeft, itemRight);
			break;
		case Subtract:
			item = new ItemFuncMinus(itemLeft, itemRight);
			break;
		case PG_And:
		case BooleanAnd:
			List<Item> argsAnd = new ArrayList<Item>();
			argsAnd.add(itemLeft);
			argsAnd.add(itemRight);
			item = new ItemCondAnd(argsAnd);
			break;
		case Concat:
		case BooleanOr:
			List<Item> argsOr = new ArrayList<Item>();
			argsOr.add(itemLeft);
			argsOr.add(itemRight);
			item = new ItemCondOr(argsOr);
			break;
		case BooleanXor:
			item = new ItemFuncXor(itemLeft, itemRight);
			break;
		case BitwiseAnd:
			item = new ItemFuncBitAnd(itemLeft, itemRight);
			break;
		case BitwiseOr:
			item = new ItemFuncBitOr(itemLeft, itemRight);
			break;
		case BitwiseXor:
			item = new ItemFuncBitXor(itemLeft, itemRight);
			break;
		case LeftShift:
			item = new ItemFuncLeftShift(itemLeft, itemRight);
			break;
		case RightShift:
			item = new ItemFuncRightShift(itemLeft, itemRight);
			break;
		case GreaterThan:
			item = new ItemFuncGt(itemLeft, itemRight);
			break;
		case GreaterThanOrEqual:
			item = new ItemFuncGe(itemLeft, itemRight);
			break;
		case NotEqual:
		case LessThanOrGreater:
			item = new ItemFuncNe(itemLeft, itemRight);
			break;
		case LessThan:
			item = new ItemFuncLt(itemLeft, itemRight);
			break;
		case LessThanOrEqual:
			item = new ItemFuncLe(itemLeft, itemRight);
			break;
		case LessThanOrEqualOrGreaterThan:
			item = new ItemFuncStrictEqual(itemLeft, itemRight);
			break;
		case RegExp:
			item = new ItemFuncRegex(itemLeft, itemRight);
			break;
		case NotRegExp:
			item = new ItemFuncRegex(itemLeft, itemRight);
			item = new ItemFuncNot(item);
			break;
		case Assignment:
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support assignment");
		default:
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported kind expression:" + x.getOperator());
		}
		initName(x);
	}

	@Override
	public void endVisit(SQLUnaryExpr x) {
		Item a = getItem(x.getExpr());
		switch(x.getOperator()){
			case Negative:
				item = new ItemFuncNeg(a);
				break;
			case Not:
			case NOT:
				item = new ItemFuncNot(a);
				break;
			case Compl:
				item = new ItemFuncBitInversion(a);
				break;
			case Plus:
				item = a;
				break;
			case BINARY:
				item = new ItemFuncBinary(a, -1);
				break;
			default:
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
						"not supported kind expression:" + x.getOperator());
		}
		initName(x);
	}
	@Override
	public void endVisit(SQLInListExpr x){
		boolean isNeg = x.isNot();
		Item left = getItem(x.getExpr());
		List<Item> args = new ArrayList<Item>();
		args.add(left);
		args.addAll(visitExprList(x.getTargetList()));
		item = new ItemFuncIn(args, isNeg);
		initName(x);
	}

	@Override
	public void endVisit(MySqlExtractExpr x){
		item = new ItemExtract(getItem(x.getValue()), x.getUnit());
		initName(x);
	}
	@Override
	public void endVisit(MySqlIntervalExpr x){
		//Just as  placeholder
		item = new ItemString(x.toString());
		item.setItemName(x.toString());
	}
	@Override
	public void endVisit(SQLNotExpr x){
		item = new ItemFuncNot(getItem(x.getExpr()));
		initName(x);
	}
	@Override
	public void endVisit(SQLAllColumnExpr x){
		item = new ItemField(null, null, "*");
		initName(x);
	}
	@Override
	public void endVisit(SQLCaseExpr x) {
		SQLExpr comparee = x.getValueExpr();
		SQLExpr elseExpr = x.getElseExpr();
		List<SQLCaseExpr.Item> whenlists= x.getItems();
		ArrayList<Item> args = new ArrayList<Item>();
		int ncases, firstExprNum = -1, elseExprNum = -1;
		for (SQLCaseExpr.Item when : whenlists) {
			args.add(getItem(when.getConditionExpr()));
			args.add(getItem(when.getValueExpr()));
		}
		ncases = args.size();
		// add comparee
		if (comparee != null) {
			firstExprNum = args.size();
			args.add(getItem(comparee));
		}
		// add else exp
		if (elseExpr != null) {
			elseExprNum = args.size();
			args.add(getItem(elseExpr));
		}
		item = new ItemFuncCase(args, ncases, firstExprNum, elseExprNum);
    }
    @Override
    public void endVisit(SQLCastExpr x) {
		Item a = getItem(x.getExpr());
		SQLDataType datetype = x.getDataType();

		if(datetype instanceof SQLCharacterDataType){
			SQLCharacterDataType charType = (SQLCharacterDataType) datetype;
			String upType = charType.getName().toUpperCase();
			List<Integer> args = changeExprListToInt(charType.getArguments());
			String charSetName = charType.getCharSetName();
			if(upType.equals("CHAR")){
				int len = -1;
				if (args.size() > 0) {
					len = args.get(0);
				}
				item = new ItemCharTypecast(a, len, charSetName);
			}
			else if(charSetName == null){
				int len = -1;
				if (args.size() > 0) {
					len = args.get(0);
				}
				item = new ItemNCharTypecast(a, len);
			}else{
				throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "",
						"You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'character set "+charSetName+")'");
			}
		}
		else{
			CastType castType = getCastType((SQLDataTypeImpl)datetype);
			item = ItemCreate.getInstance().create_func_cast(a, castType);
		}
		initName(x);
    }
    @Override
    public void endVisit(SQLCharExpr x) {
    	item = new ItemString(x.getText());
    	item.charsetIndex=this.charsetIndex;
		initName(x);
    }
    @Override
	public void endVisit(SQLIdentifierExpr x) {
		item = new ItemField(null, null, StringUtil.removeBackQuote(x.getSimpleName()));
	}
	@Override
	public void endVisit(SQLNullExpr x){
		item = new ItemNull();
		initName(x);
	}
	@Override
    public void endVisit(SQLIntegerExpr x) {
    	Number number = x.getNumber();
    	item = new ItemInt(number.longValue());
    	initName(x);
    }
	@Override
    public void endVisit(SQLNCharExpr x) {
    	item = new ItemString(x.getText());
    	item.charsetIndex=this.charsetIndex;
		initName(x);
    }
	@Override
    public void endVisit(SQLNumberExpr x) {
    	Number number = x.getNumber();
    	if (number instanceof BigDecimal) {
			item = new ItemFloat((BigDecimal) number);
		} else {
			item = new ItemInt(number.longValue());
		}
    	initName(x);
    }
	@Override
	public void endVisit(SQLPropertyExpr x) {
		SQLIdentifierExpr owner= (SQLIdentifierExpr) x.getOwner();
		item = new ItemField(null, StringUtil.removeBackQuote(owner.getSimpleName()), StringUtil.removeBackQuote(x.getSimpleName()));
	}

	@Override
	public void endVisit(SQLAggregateExpr x) {
		List<Item> args = visitExprList(x.getArguments());
		String funcName = x.getMethodName().toUpperCase();
		SQLAggregateOption option = x.getOption();
		boolean isDistinct = option == null ? false : true;
		switch(funcName){
		case "MAX":
			item = new ItemSumMax(args, false, null);
			break;
		case "MIN":
			item = new ItemSumMin(args, false, null);
			break;
		case "SUM":
			item = new ItemSumSum(args,isDistinct, false, null);
			break;
		case "AVG":
			item = new ItemSumAvg(args,isDistinct, false, null);
			break;
		case "GROUP_CONCAT":
			SQLOrderBy orderExpr = (SQLOrderBy) x.getAttribute(ItemFuncKeyWord.ORDER_BY);
			List<Order> orderList = null;
			if (orderExpr != null) {
				orderList = new ArrayList<Order>();
				for (SQLSelectOrderByItem orderItem : orderExpr.getItems()) {
					Order order = new Order(getItem(orderItem.getExpr()), orderItem.getType());
					orderList.add(order);
				}
			}
			SQLCharExpr charExpr = (SQLCharExpr) x.getAttribute(ItemFuncKeyWord.SEPARATOR);
			String separator = ",";
			if (charExpr != null) {
				separator = charExpr.getText();
			}
			item = new ItemFuncGroupConcat(args, isDistinct, orderList, separator, false, null);
			break;
		case "COUNT":
			item = new ItemSumCount(args,isDistinct, false, null);
			break;
		case "STDDEV":
			item =  new ItemSumStd(args, 0, false, null);
			break;
		}
    }
	@Override
	public void endVisit(SQLMethodInvokeExpr x) {
		List<Item> args = visitExprList(x.getParameters());
		String funcName = x.getMethodName().toUpperCase();
		Map<String, Object> attributes = x.getAttributes();
		switch (funcName) {
			case "TRIM":
				if (attributes == null) {
					item = new ItemFuncTrim(args.get(0), TRIM_TYPE_ENUM.DEFAULT);
				} else {
					TRIM_TYPE_ENUM trimType = TRIM_TYPE_ENUM.DEFAULT;
					String type = (String) attributes.get(ItemFuncKeyWord.TRIM_TYPE);
					if (type != null) {
						trimType = TRIM_TYPE_ENUM.valueOf(type);
					}
					if (attributes.get(ItemFuncKeyWord.FROM) == null) {
						item = new ItemFuncTrim(args.get(0), trimType);
					} else {
						SQLCharExpr from = (SQLCharExpr) attributes.get(ItemFuncKeyWord.FROM);
						item = new ItemFuncTrim(args.get(0), getItem(from), trimType);
					}
				}
				break;
			case "CONVERT":
				if (args.size() >= 2) {
					throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported  CONVERT(expr, type) ,please use CAST(expr AS type)");
				}
				if (attributes == null || attributes.get(ItemFuncKeyWord.USING) == null) {
					throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "CONVERT(... USING ...) is standard SQL syntax");
				}
				item = new ItemFuncConvCharset(args.get(0), (String) attributes.get(ItemFuncKeyWord.USING));
				break;
			case "CHAR":
				if (attributes == null || attributes.get(ItemFuncKeyWord.USING) == null) {
					attributes = x.getParameters().get(0).getAttributes();
				}
				if (attributes == null || attributes.get(ItemFuncKeyWord.USING) == null) {
					item = new ItemFuncChar(args,this.charsetIndex);
				} else {
					item = new ItemFuncChar(args, (String) attributes.get(ItemFuncKeyWord.USING));
				}
				break;
			case "ORD":
				item = new ItemFuncOrd(args,this.charsetIndex);
				break;
			case "ADDDATE":
				if (x.getParameters().get(1) instanceof SQLIntegerExpr) {
					item = new ItemDateAddInterval(args.get(0), args.get(1), MySqlIntervalUnit.DAY, false);
					break;
				}
			case "DATE_ADD":
				MySqlIntervalExpr intervalExpr = (MySqlIntervalExpr) (x.getParameters().get(1));
				item = new ItemDateAddInterval(args.get(0), getItem(intervalExpr.getValue()), getIntervalUint(x.getParameters().get(1)), false);
				break;
			case "SUBDATE":
				if (x.getParameters().get(1) instanceof SQLIntegerExpr) {
					item = new ItemDateAddInterval(args.get(0), args.get(1), MySqlIntervalUnit.DAY, true);
					break;
				}
			case "DATE_SUB":
				MySqlIntervalExpr valueExpr = (MySqlIntervalExpr) (x.getParameters().get(1));
				item = new ItemDateAddInterval(args.get(0), getItem(valueExpr.getValue()), getIntervalUint(x.getParameters().get(1)), true);
				break;
			case "TIMESTAMPADD":
				SQLIdentifierExpr addUnit = (SQLIdentifierExpr) x.getParameters().get(0);
				item = new ItemDateAddInterval(args.get(2), args.get(1), MySqlIntervalUnit.valueOf(addUnit.getSimpleName()), false);
				break;
			case "TIMESTAMPDIFF":
				SQLIdentifierExpr diffUnit = (SQLIdentifierExpr) x.getParameters().get(0);
				item = new ItemFuncTimestampDiff(args.get(1), args.get(2), MySqlIntervalUnit.valueOf(diffUnit.getSimpleName()));
				break;
			case "VAR_SAMP":
				item = new ItemSumVariance(args, 1, false, null);
				break;
			case "VAR_POP":
			case "VARIANCE":
				item = new ItemSumVariance(args, 0, false, null);
				break;
			case "STD":
			case "STDDEV":
			case "STDDEV_POP":
				item = new ItemSumStd(args, 0, false, null);
				break;
			case "STDDEV_SAMP":
				item = new ItemSumStd(args, 1, false, null);
				break;
			case "BIT_AND":
				item = new ItemSumAnd(args, false, null);
				break;
			case "BIT_OR":
				item = new ItemSumOr(args, false, null);
				break;
			case "BIT_XOR":
				item = new ItemSumXor(args, false, null);
				break;
			case "IF":
				item = new ItemFuncIf(args);
				break;
			case "GET_FORMAT":
				SQLExpr expr = x.getParameters().get(0);
				if(expr instanceof SQLIdentifierExpr){
					Item arg0 = new ItemString(((SQLIdentifierExpr) expr).getName());
					args.set(0,arg0);
				}else{
					throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '"+expr.toString()+"'");
				}
				item = ItemCreate.getInstance().createNativeFunc(funcName, args);
				break;
			default:
				if (ItemCreate.getInstance().isNativeFunc(funcName)) {
					item = ItemCreate.getInstance().createNativeFunc(funcName, args);
				} else {
					// unKnownFunction
					item = new ItemFuncUnknown(funcName, args);
				}
				initName(x);
		}
	}


	@Override
	public void endVisit(SQLListExpr x) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Row Subqueries is not supported");
	}

	@Override
	public void endVisit(SQLAllExpr x) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Subqueries with All is not supported");
	}

	@Override
	public void endVisit(SQLSomeExpr x) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Subqueries with Some is not supported");
	}


	@Override
	public void endVisit(SQLAnyExpr x) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Subqueries with Any is not supported");
	}


	@Override
	public void endVisit(SQLExistsExpr x) {
		// TODO
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported exists!");
    }
	@Override
	public void endVisit(SQLBinaryExpr x) {
		String binary = x.getValue();
		item = new ItemInt(Long.parseLong(binary,2));
		initName(x);
	}

	@Override
    public void endVisit(SQLHexExpr x) {
		byte[] bytes = x.toBytes();
		if (bytes ==null) {
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'X'" + x.getHex() + "''");
		}
		try {
			item = new ItemString(new String(bytes,CharsetUtil.getJavaCharset(this.charsetIndex)));
		} catch (UnsupportedEncodingException e) {
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Not Support charset index ="+this.charsetIndex);
		}

	}

	@Override
	public void endVisit(SQLSelectStatement node) {
		SQLSelectQuery sqlSelect = node.getSelect().getQuery();
		item = new ItemSinglerowSubselect(currentDb, sqlSelect);
	}

	private CastType getCastType(SQLDataTypeImpl dataTypeImpl) {
		CastType castType = new CastType();
		String upType = dataTypeImpl.getName().toUpperCase();
		List<Integer> args = changeExprListToInt(dataTypeImpl.getArguments());
		if (upType.equals("BINARY")) {
			castType.target = CastTarget.ITEM_CAST_BINARY;
			if (args.size() > 0) {
				castType.length = args.get(0);
			}
		} else if (upType.equals("DATE")) {
			castType.target = CastTarget.ITEM_CAST_DATE;
		} else if (upType.equals("DATETIME")) {
			castType.target = CastTarget.ITEM_CAST_DATETIME;
			if (args.size() > 0) {
				castType.length = args.get(0);
			}
		} else if (upType.equals("DECIMAL")) {
			castType.target = CastTarget.ITEM_CAST_DECIMAL;
			if (args.size() > 0) {
				castType.length = args.get(0);
			}
			if (args.size() > 1) {
				castType.dec = args.get(1);
			}
		} else if (upType.equals("NCHAR")) {
			castType.target = CastTarget.ITEM_CAST_NCHAR;
			if (args.size() > 0) {
				castType.length = args.get(0);
			}
		} else if (upType.equals("SIGNED")) {
			castType.target = CastTarget.ITEM_CAST_SIGNED_INT;
		} else if (upType.equals("UNSIGNED")) {
			castType.target = CastTarget.ITEM_CAST_UNSIGNED_INT;
		} else if (upType.equals("TIME")) {
			castType.target = CastTarget.ITEM_CAST_TIME;
			if (args.size() > 0) {
				castType.length = args.get(0);
			}
		} else {
			// not support SIGNED INT /UNSIGNED INT/JSON
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported cast as:" + upType);
		}
		return castType;
	}
	private List<Integer> changeExprListToInt(List<SQLExpr> exprList) {
		List<Integer> args = new ArrayList<Integer>();
		for (SQLExpr expr : exprList) {
			Number num = ((SQLNumericLiteralExpr)expr).getNumber();
			args.add(num.intValue());
		}
		return args;
	}
	private List<Item> visitExprList(List<SQLExpr> exprList) {
		List<Item> args = new ArrayList<Item>();
		for (SQLExpr expr : exprList) {
			args.add(getItem(expr));
		}
		return args;
	}
	private Item getItem(SQLExpr expr){
		MySQLItemVisitor fv = new MySQLItemVisitor(currentDb, this.charsetIndex);
		expr.accept(fv);
		return fv.getItem();
	}
	private MySqlIntervalUnit getIntervalUint(SQLExpr expr){
		return ((MySqlIntervalExpr)expr).getUnit();
	}
	private void initName(SQLExpr expr) {
		StringBuilder sb = new StringBuilder();
		MySqlOutputVisitor ov = new MySqlOutputVisitor(sb);
		expr.accept(ov);
		item.setItemName(sb.toString());
	}
}
