package io.mycat.plan.common.item.function.sumfunc;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.external.ResultStore;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.field.FieldUtil;


public class AggregatorDistinct extends Aggregator {
	private boolean endup_done = false;
	private final ResultStore distinctRows;
	private Field field = null;

	public AggregatorDistinct(ItemSum arg, ResultStore store) {
		super(arg);
		this.distinctRows = store;
	}

	@Override
	public AggregatorType Aggrtype() {
		return AggregatorType.DISTINCT_AGGREGATOR;
	}

	/***************************************************************************/
	/**
	 * Called before feeding the first row. Used to allocate/setup the internal
	 * structures used for aggregation.
	 * 
	 * @param thd
	 *            Thread descriptor
	 * @return status
	 * @throws UnsupportedEncodingException 
	 * @retval FALSE success
	 * @retval TRUE faliure
	 * 
	 *         Prepares Aggregator_distinct to process the incoming stream.
	 *         Creates the temporary table and the Unique class if needed.
	 *         Called by Item_sum::aggregator_setup()
	 */
	@Override
	public boolean setup(){
		endup_done = false;
		if (item_sum.setup())
			return true;
		// TODO see item_sum.cc for more
		FieldPacket tmp = new FieldPacket();
		item_sum.getArg(0).makeField(tmp);
		field = Field.getFieldItem(tmp.name, tmp.table, tmp.type, tmp.charsetIndex, (int) tmp.length, tmp.decimals,
					tmp.flags);
		return false;
	}

	@Override
	public void clear() {
		endup_done = false;
		distinctRows.clear();
		if (item_sum.sumType() == ItemSum.Sumfunctype.COUNT_FUNC
				|| item_sum.sumType() == ItemSum.Sumfunctype.COUNT_DISTINCT_FUNC) {

		} else {
			item_sum.nullValue = true;
		}
	}

	/**
	 * add the distinct value into buffer, to use when endup() is called
	 */
	@Override
	public boolean add(RowDataPacket row, Object transObj) {
		distinctRows.add(row);
		return false;
	}

	@Override
	public void endup() {
		if (endup_done)
			return;
		item_sum.clear();
		if (distinctRows != null) {
			distinctRows.done();
			if (distinctRows != null && !endup_done) {
				use_distinct_values = true;
				RowDataPacket row = null;
				while ((row = distinctRows.next()) != null) {
					// @bug1072 see arg_is_null()
					FieldUtil.initFields(item_sum.sourceFields, row.fieldValues);
					field.setPtr(item_sum.getArg(0).getRowPacketByte());
					if (item_sum.isPushDown)
						item_sum.pushDownAdd(row);
					else
						item_sum.add(row, null);
				}
				use_distinct_values = false;
			}
			endup_done = true;
		}
	}

	@Override
	public BigDecimal arg_val_decimal() {
		return use_distinct_values ? field.valDecimal() : item_sum.getArg(0).valDecimal();
	}

	@Override
	public BigDecimal arg_val_real() {
		return use_distinct_values ? field.valReal() : item_sum.getArg(0).valReal();
	}

	@Override
	public boolean arg_is_null() {
		return use_distinct_values ? field.isNull() : item_sum.getArg(0).nullValue;
	}

}
