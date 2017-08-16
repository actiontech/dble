package io.mycat.backend.mysql.nio.handler.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.mycat.util.StringUtil;
import org.apache.log4j.Logger;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.MysqlVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.field.FieldUtil;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.ItemRef;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;

public class HandlerTool {
	// 两端是单引号，并且中间不允许出现单引号
		// private static Pattern pat = Pattern.compile("^\'([^\']*?)\'$");

		/**
		 * 停止以node为根节点的handler树
		 * 
		 * @param node
		 */
		public static void terminateHandlerTree(final DMLResponseHandler node) {
			try {
				if (node == null)
					return;
				Set<DMLResponseHandler> merges = node.getMerges();
				for (DMLResponseHandler merge : merges) {
					DMLResponseHandler currentHandler = merge;
					while (currentHandler != node) {
						currentHandler.terminate();
						currentHandler = currentHandler.getNextHandler();
					}
				}
				node.terminate();
			} catch (Exception e) {
				Logger.getLogger(HandlerTool.class).error("terminate node exception:", e);
			}
		}

//		public static byte[] getEofBytes(MySQLConnection conn) {
//			EOFPacket eof = new EOFPacket();
//			return eof.toByteBuffer(conn.getCharset()).array();
//		}

		public static Field createField(FieldPacket fp) {
			Field field = Field.getFieldItem(fp.name, fp.table, fp.type, fp.charsetIndex, (int) fp.length, fp.decimals,
					fp.flags);
			return field;
		}

		public static List<Field> createFields(List<FieldPacket> fps) {
			List<Field> ret = new ArrayList<Field>();
			for (FieldPacket fp : fps) {
				Field field = createField(fp);
				ret.add(field);
			}
			return ret;
		}

		/**
		 * 创建一个Item，并且Item内部的对象指向fields中的某个对象，当field的实际值改变时，Item的value也改变
		 * 
		 * @param sel
		 * @param fields
		 * @param type
		 * @return
		 */
		public static Item createItem(Item sel, List<Field> fields, int startIndex, boolean allPushDown, HandlerType type,
				String charset) {
			Item ret;
			if (sel.basicConstItem())
				return sel;
			Item.ItemType i = sel.type();
			if (i == Item.ItemType.FUNC_ITEM || i == Item.ItemType.COND_ITEM) {
				ItemFunc func = (ItemFunc) sel;
				if (func.getPushDownName() == null || func.getPushDownName().length() == 0) {
					// 自己计算
					ret = createFunctionItem(func, fields, startIndex, allPushDown, type, charset);
				} else {
					ret = createFieldItem(func, fields, startIndex);
				}

			} else if (i == Item.ItemType.SUM_FUNC_ITEM) {
				ItemSum sumFunc = (ItemSum) sel;
				if (type != HandlerType.GROUPBY) {
					ret = createFieldItem(sumFunc, fields, startIndex);
				} else if (sumFunc.getPushDownName() == null || sumFunc.getPushDownName().length() == 0) {
					ret = createSumItem(sumFunc, fields, startIndex, allPushDown, type, charset);
				} else {
					ret = createPushDownGroupBy(sumFunc, fields, startIndex);
				}

			} else {
				ret = createFieldItem(sel, fields, startIndex);
			}
			ret.fixFields();
			return ret;
		}

		public static Item createRefItem(Item ref, String tbAlias, String fieldAlias) {
			return new ItemRef(ref, tbAlias, fieldAlias);
		}

		/**
		 * 将field进行复制
		 * 
		 * @param fields
		 * @param bs
		 */
		public static void initFields(List<Field> fields, List<byte[]> bs) {
			FieldUtil.initFields(fields, bs);
		}

		public static List<byte[]> getItemListBytes(List<Item> items) {
			List<byte[]> ret = new ArrayList<>(items.size());
			for (Item item : items) {
				byte[] b = item.getRowPacketByte();
				ret.add(b);
			}
			return ret;
		}

		public static ItemField createItemField(FieldPacket fp) {
			Field field = createField(fp);
			return new ItemField(field);
		}

		/*
		 * ------------------------------- helper methods ------------------------
		 */
		/**
		 * 计算下发的聚合函数 1.count(id) 下发count(id) 之后 count(id) = sum[count(id) 0...n];
		 * 2.sum(id) sum(id) = sum[sum(id) 0...n]; 3.avg(id) avg(id) = sum[sum(id)
		 * 0...n]/sum[count(id) 0...n];
		 * 
		 * @param sumfun
		 *            聚合函数的名称
		 * @param fields
		 *            当前所有行的fields信息
		 * @return
		 */
		protected static Item createPushDownGroupBy(ItemSum sumfun, List<Field> fields, int startIndex) {
			String funName = sumfun.funcName().toUpperCase();
			String colName = sumfun.getItemName();
			String pdName = sumfun.getPushDownName();
			Item ret = null;
			List<Item> args = new ArrayList<Item>();
			if (funName.equalsIgnoreCase("AVG")) {
				String colNameSum = colName.replace(funName + "(", "SUM(");
				String colNameCount = colName.replace(funName + "(", "COUNT(");
				Item sumfunSum = new ItemField(null, null, colNameSum);
				sumfunSum.setPushDownName(
						pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
				Item sumfunCount = new ItemField(null, null, colNameCount);
				sumfunCount.setPushDownName(
						pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
				Item itemSum = createFieldItem(sumfunSum, fields, startIndex);
				Item itemCount = createFieldItem(sumfunCount, fields, startIndex);
				args.add(itemSum);
				args.add(itemCount);
			} else if (funName.equalsIgnoreCase("STD") || funName.equalsIgnoreCase("STDDEV_POP")
					|| funName.equalsIgnoreCase("STDDEV_SAMP") || funName.equalsIgnoreCase("STDDEV")
					|| funName.equalsIgnoreCase("VAR_POP") || funName.equalsIgnoreCase("VAR_SAMP")
					|| funName.equalsIgnoreCase("VARIANCE")) {
				// variance:下发时 v[0]:count,v[1]:sum,v[2]:variance(局部)
				String colNameCount = colName.replace(funName + "(", "COUNT(");
				String colNameSum = colName.replace(funName + "(", "SUM(");
				String colNameVar = colName.replace(funName + "(", "VARIANCE(");
				Item sumfunCount = new ItemField(null, null, colNameCount);
				sumfunCount.setPushDownName(
						pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
				Item sumfunSum = new ItemField(null, null, colNameSum);
				sumfunSum.setPushDownName(
						pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
				Item sumfunVar = new ItemField(null, null, colNameVar);
				sumfunVar.setPushDownName(
						pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("VARIANCE")));
				Item itemCount = createFieldItem(sumfunCount, fields, startIndex);
				Item itemSum = createFieldItem(sumfunSum, fields, startIndex);
				Item itemVar = createFieldItem(sumfunVar, fields, startIndex);
				args.add(itemCount);
				args.add(itemSum);
				args.add(itemVar);
			} else {
				Item subItem = createFieldItem(sumfun, fields, startIndex);
				args.add(subItem);
			}
			ret = sumfun.reStruct(args, true, fields);
			ret.setItemName(sumfun.getPushDownName() == null ? sumfun.getItemName() : sumfun.getPushDownName());
			return ret;
		}

		protected static ItemFunc createFunctionItem(ItemFunc f, List<Field> fields, int startIndex, boolean allPushDown,
				HandlerType type, String charset) {
			ItemFunc ret = null;
			List<Item> args = new ArrayList<Item>();
			for (int index = 0; index < f.getArgCount(); index++) {
				Item arg = f.arguments().get(index);
				Item newArg = null;
				if (arg.isWild())
					newArg = new ItemInt(0);
				else
					newArg = createItem(arg, fields, startIndex, allPushDown, type, charset);
				if (newArg == null)
					throw new RuntimeException("Function argument not found:" + arg);
				args.add(newArg);
			}
			ret = (ItemFunc) f.reStruct(args, allPushDown, fields);
			ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
			return ret;
		}

		/**
		 * @param func
		 * @param fields
		 * @param startIndex
		 * @param allPushDown
		 * @param type
		 * @param charset
		 * @return
		 */
		private static ItemSum createSumItem(ItemSum f, List<Field> fields, int startIndex, boolean allPushDown,
				HandlerType type, String charset) {
			ItemSum ret = null;
			List<Item> args = new ArrayList<Item>();
			for (int index = 0; index < f.getArgCount(); index++) {
				Item arg = f.arguments().get(index);
				Item newArg = null;
				if (arg.isWild())
					newArg = new ItemInt(0);
				else
					newArg = createItem(arg, fields, startIndex, allPushDown, type, charset);
				if (newArg == null)
					throw new RuntimeException("Function argument not found:" + arg);
				args.add(newArg);
			}
			ret = (ItemSum) f.reStruct(args, allPushDown, fields);
			ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
			return ret;
		}

		/**
		 * 查出col对应的field，所有的col对象不管是函数还是非函数均当做普通列处理，直接比较他们的表名和列名
		 * 
		 * @param col
		 * @param fields
		 * @return
		 */
		protected static ItemField createFieldItem(Item col, List<Field> fields, int startIndex) {
			int index = findField(col, fields, startIndex);
			if (index < 0)
				throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "field not found:" + col);
			ItemField ret = new ItemField(fields.get(index));
			ret.setItemName(col.getPushDownName() == null ? col.getItemName() : col.getPushDownName());
			return ret;
		}

		/**
		 * 查找sel在fields中的对应，包含start，
		 * 
		 * @param sel
		 * @param fields
		 * @param startIndex
		 * @param endIndex
		 * @return
		 */
		public static int findField(Item sel, List<Field> fields, int startIndex) {
			String selName = (sel.getPushDownName() == null ? sel.getItemName() : sel.getPushDownName());
			selName = selName.trim();
			String tableName = sel.getTableName();
			for (int index = startIndex; index < fields.size(); index++) {
				Field field = fields.get(index);
				// ''下发之后field.name==null
				String colName2 = field.name == null ? null : field.name.trim();
				String tableName2 = field.table;
				if (sel instanceof ItemField && !((StringUtil.isEmpty(tableName) && StringUtil.isEmpty(tableName2))||tableName.equals(tableName2)))
					continue;
				if (selName.equalsIgnoreCase(colName2))
					return index;
			}
			return -1;
		}

		/**
		 * 根据distinct的列生成orderby
		 * 
		 * @param sels
		 * @return
		 */
		public static List<Order> makeOrder(List<Item> sels) {
			List<Order> orders = new ArrayList<Order>();
			for (Item sel : sels) {
				Order order = new Order(sel, SQLOrderingSpecification.ASC);
				orders.add(order);
			}
			return orders;
		}

		// @bug 1086
		public static boolean needSendNoRow(List<Order> groupBys) {
			if (groupBys == null || groupBys.size() == 0) {
				return true;
			} else {
				return false;
			}
		}
}
