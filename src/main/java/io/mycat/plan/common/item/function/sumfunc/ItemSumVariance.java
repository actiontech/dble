package io.mycat.plan.common.item.function.sumfunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.DoublePtr;
import io.mycat.plan.common.ptr.LongPtr;


/**
 * /* variance(a) =
 * 
 * = sum (ai - avg(a))^2 / count(a) ) = sum (ai^2 - 2*ai*avg(a) + avg(a)^2) /
 * count(a) = (sum(ai^2) - sum(2*ai*avg(a)) + sum(avg(a)^2))/count(a) = =
 * (sum(ai^2) - 2*avg(a)*sum(a) + count(a)*avg(a)^2)/count(a) = = (sum(ai^2) -
 * 2*sum(a)*sum(a)/count(a) + count(a)*sum(a)^2/count(a)^2 )/count(a) = =
 * (sum(ai^2) - 2*sum(a)^2/count(a) + sum(a)^2/count(a) )/count(a) = =
 * (sum(ai^2) - sum(a)^2/count(a))/count(a)
 * 
 * But, this falls prey to catastrophic cancellation. Instead, use the
 * recurrence formulas
 * 
 * M_{1} = x_{1}, ~ M_{k} = M_{k-1} + (x_{k} - M_{k-1}) / k S_{1} = 0, ~S_{k} =
 * S_{k-1} + (x_{k} - M_{k-1}) times (x_{k} - M_{k}) for 2 <= k <= n ital
 * variance = S_{n} / (n-1)
 * 
 * 
 * @author ActionTech
 * 
 */

public class ItemSumVariance extends ItemSumNum {
	public ItemResult hybrid_type;
	public double recurrence_m, recurrence_s; /* Used in recurrence relation. */
	public long count = 0;
	public int sample;

	// pushdown variables下发的情况下计算时所需要用到的变量
	// 下发时 v[0]:count,v[1]:sum,v[2]:variance(局部)
	// 依据为 variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
	private double sum = 0;
	private double squareSum = 0;

	/** 为了局部聚合时使用 **/
	private boolean useTransObj = false;
	// 依据为 variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
	private double sumAi2 = 0;
	private double sumA = 0;

	private static class AggData implements Serializable {

		private static final long serialVersionUID = -5441804522036055390L;

		public double sumAi2;
		public double sumA;
		public long count;

		public AggData(double sumAi2, double sumA, long count) {
			this.sumAi2 = sumAi2;
			this.sumA = sumA;
			this.count = count;
		}

	}

	public ItemSumVariance(List<Item> args, int sample, boolean isPushDown, List<Field> fields) {
		super(args, isPushDown, fields);
		this.sample = sample;
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = nullValue = true;
		hybrid_type = ItemResult.REAL_RESULT;
		decimals = NOT_FIXED_DEC;
		maxLength = floatLength(decimals);
	}

	@Override
	public Sumfunctype sumType() {
		return Sumfunctype.VARIANCE_FUNC;
	}

	@Override
	public void clear() {
		count = 0;
		sum = squareSum = 0.0;
		sumA = sumAi2 = 0.0;
		useTransObj = false;
	}

	@Override
	public Object getTransAggObj() {
		AggData data = new AggData(sumAi2, sumA, count);
		return data;
	}

	@Override
	public int getTransSize() {
		return 20;
	}

	@Override
	public boolean add(RowDataPacket row, Object transObj) {
		if (transObj != null) {
			useTransObj = true;
			AggData other = (AggData) transObj;
			sumAi2 += other.sumAi2;
			sumA += other.sumA;
			count += other.count;
		} else {
			/*
			 * Why use a temporary variable? We don't know if it is null until
			 * we evaluate it, which has the side-effect of setting null_value .
			 */
			double nr = args.get(0).valReal().doubleValue();
			// add for transObj
			sumA += nr;
			sumAi2 += nr * nr;
			// end add
			if (!args.get(0).nullValue) {
				DoublePtr r_m = new DoublePtr(recurrence_m);
				DoublePtr r_s = new DoublePtr(recurrence_s);
				LongPtr countPtr = new LongPtr(count);
				varianceFpRecurrenceNext(r_m, r_s, countPtr, nr);
				recurrence_m = r_m.get();
				recurrence_s = r_s.get();
				count = countPtr.get();
			}
		}
		return false;
	}

	// pushdown variables下发的情况下计算时所需要用到的变量
	// 下发时 v[0]:count,v[1]:sum,v[2]:variance(局部)
	// 依据为 variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
	@Override
	public boolean pushDownAdd(RowDataPacket row) {
		// 下发的做法,依据为 variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
		long partCount = args.get(0).valInt().longValue();
		double partSum = args.get(1).valReal().doubleValue();
		double partVariane = args.get(2).valReal().doubleValue();
		if (partCount != 0) {
		    	count += partCount;
			double partSqarSum = partVariane * partCount + partSum * partSum / partCount;
			squareSum += partSqarSum;
			sum += partSum;
		}
		
		return false;
	}

	@Override
	public BigDecimal valReal() {
		if (!isPushDown) {
			/*
			 * 'sample' is a 1/0 boolean value. If it is 1/true, id est this is
			 * a sample variance call, then we should set nullness when the
			 * count of the items is one or zero. If it's zero, i.e. a
			 * population variance, then we only set nullness when the count is
			 * zero.
			 * 
			 * Another way to read it is that 'sample' is the numerical
			 * threshhold, at and below which a 'count' number of items is
			 * called NULL.
			 */
			assert ((sample == 0) || (sample == 1));
			if (count <= sample) {
				nullValue = true;
				return BigDecimal.ZERO;
			}

			nullValue = false;
			if (!useTransObj) {
				double db = varianceFpRecurrenceResult(recurrence_s, count, sample != 0);
				return BigDecimal.valueOf(db);
			} else {
				double db = (sumAi2 - sumA * sumA / count) / (count - sample);
				return BigDecimal.valueOf(db);
			}
		} else {
			double db = pushDownVal();
			return BigDecimal.valueOf(db);
		}
	}

	private double pushDownVal() {
		if (count <= sample) {
			nullValue = true;
			return 0.0;
		}
		nullValue = false;
		if (count == 1)
			return 0.0;

		double s = (squareSum - sum * sum / count);

		if (sample == 1)
			return s / (count - 1);
		else
			return s / count;

	}

	@Override
	public BigDecimal valDecimal() {
		return valDecimalFromReal();
	}

	@Override
	public void noRowsInResult() {
	}

	@Override
	public String funcName() {
		return sample == 1 ? "VAR_SAMP" : "VARIANCE";
	}

	@Override
	public void cleanup() {
		count = 0;
		super.cleanup();
	}

	@Override
	public ItemResult resultType() {
		return ItemResult.REAL_RESULT;
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
		if (!forCalculate) {
			List<Item> newArgs = cloneStructList(args);
			return new ItemSumVariance(newArgs, sample, false, null);
		} else {
			return new ItemSumVariance(calArgs, sample, isPushDown, fields);
		}
	}
}
