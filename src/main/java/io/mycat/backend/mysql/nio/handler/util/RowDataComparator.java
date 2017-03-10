package io.mycat.backend.mysql.nio.handler.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;

/**
 * 根据OrderBy的item list进行行数据排序的比较器
 * 
 * 
 */
public class RowDataComparator implements Comparator<RowDataPacket> {

	private List<Field> sourceFields;
	private List<Item> cmpItems;

	private List<Field> cmpFields;
	private List<Boolean> ascs;

	// only for test unit
	public RowDataComparator(List<Field> sourceFields, List<Item> cmpItems, List<Field> cmpFields, List<Boolean> ascs,
			String charset) {
		this.sourceFields = sourceFields;
		this.cmpItems = cmpItems;
		this.cmpFields = cmpFields;
		this.ascs = ascs;
	}

	public RowDataComparator(List<FieldPacket> fps, List<Order> orders, boolean allPushDown, HandlerType type,
			String charset) {
		sourceFields = HandlerTool.createFields(fps);
		if (orders != null && orders.size() > 0) {
			ascs = new ArrayList<Boolean>();
			cmpFields = new ArrayList<Field>();
			cmpItems = new ArrayList<Item>();
			for (Order order : orders) {
				Item cmpItem = HandlerTool.createItem(order.getItem(), sourceFields, 0, allPushDown, type, charset);
				cmpItems.add(cmpItem);
				FieldPacket tmpFp = new FieldPacket();
				cmpItem.makeField(tmpFp);
				Field cmpField = HandlerTool.createField(tmpFp);
				cmpFields.add(cmpField);
				ascs.add(order.getSortOrder() == SQLOrderingSpecification.ASC ? true : false);
			}
		}
	}

	public int getSourceFieldCount() {
		return sourceFields.size();
	}

	public void sort(List<RowDataPacket> rows) {
		Comparator<RowDataPacket> c = new Comparator<RowDataPacket>() {

			@Override
			public int compare(RowDataPacket o1, RowDataPacket o2) {
				if (RowDataComparator.this.ascs != null && RowDataComparator.this.ascs.size() > 0)
					return RowDataComparator.this.compare(o1, o2);
				else
					// 无须比较，按照原始的数据输出
					return -1;
			}
		};
		Collections.sort(rows, c);
	}

	@Override
	/**
	 * 传递进来的是源生行的row数据
	 */
	public int compare(RowDataPacket o1, RowDataPacket o2) {
		if (this.ascs != null && this.ascs.size() > 0) {
			int cmpValue = cmp(o1, o2, 0);
			return cmpValue;
		} else
			// 无须比较，按照原始的数据输出
			return 0;
	}

	private int cmp(RowDataPacket o1, RowDataPacket o2, int index) {
		HandlerTool.initFields(sourceFields, o1.fieldValues);
		List<byte[]> bo1 = HandlerTool.getItemListBytes(cmpItems);
		HandlerTool.initFields(sourceFields, o2.fieldValues);
		List<byte[]> bo2 = HandlerTool.getItemListBytes(cmpItems);
		boolean isAsc = ascs.get(index);
		Field field = cmpFields.get(index);
		byte[] b1 = bo1.get(index);
		byte[] b2 = bo2.get(index);
		int rs;
		if (isAsc) {
			rs = field.compare(b1, b2);
		} else {
			rs = field.compare(b2, b1);
		}
		if (rs != 0 || cmpFields.size() == (index + 1)) {
			return rs;
		} else {
			return cmp(o1, o2, index + 1);
		}
	}

}