package io.mycat.plan.node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLHint;

import io.mycat.MycatServer;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.TableConfig.TableTypeEnum;
import io.mycat.meta.protocol.MyCatMeta.ColumnMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.plan.NamedField;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.util.ToStringUtil;

public class TableNode extends PlanNode {

	public PlanNodeType type() {
		return PlanNodeType.TABLE;
	}

	private String schema;
	private String tableName;
	private TableMeta tableMeta;
	private List<SQLHint> hintList;

	/**
	 * @param areaSchema
	 * @param tableName
	 */
	public TableNode(String catalog, String tableName) {
		if (catalog == null || tableName == null)
			throw new RuntimeException("Table db or name is null error!");
		this.schema = catalog;
		this.tableName = tableName;
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig(); 
		if(mycatConfig.getSystem().isLowerCaseTableNames()){
			this.schema = this.schema.toLowerCase();
			this.tableName = this.tableName.toLowerCase();
		}
		SchemaConfig schemaConfig = mycatConfig.getSchemas().get(this.schema);
		if(schemaConfig == null){
			throw new RuntimeException("schema "+this.schema+" is not exists!");
		}
		this.referedTableNodes.add(this);

		
		this.tableMeta = MycatServer.getInstance().getTmManager().getSyncTableMeta(this.schema, this.tableName);
		TableConfig tableConfig = schemaConfig.getTables().get(this.tableName);
		boolean isGlobaled = tableConfig != null && (tableConfig.getTableType() == TableTypeEnum.TYPE_GLOBAL_TABLE);
		if (!isGlobaled) {
			this.unGlobalTableCount = 1;
		}
		this.setNoshardNode(new HashSet<String>(tableConfig.getDataNodes()));
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	protected void setUpInnerFields() {
		innerFields.clear();
		String tmpTable = subAlias == null ? tableName : subAlias;
		for (ColumnMeta cm : tableMeta.getColumnsList()) {
			NamedField tmpField = new NamedField(tmpTable, cm.getName(), this);
			innerFields.put(tmpField, tmpField);
		}
	}

	@Override
	protected void dealStarColumn() {
		List<Item> newSels = new ArrayList<Item>();
		for (Item sel : columnsSelected) {
			if (!sel.isWild())
				newSels.add(sel);
			else {
				for (NamedField innerField : innerFields.keySet()) {
					ItemField col = new ItemField(null, sel.getTableName(), innerField.name);
					newSels.add(col);
				}
			}
		}
		columnsSelected = newSels;
	}

	public TableNode copy() {
		TableNode newTableNode = new TableNode(schema, tableName);
		this.copySelfTo(newTableNode);
		newTableNode.setHintList(this.hintList);
		return newTableNode;
	}

	@Override
	public String getPureName() {
		return this.getTableName();
	}

	public String getSchema() {
		return this.schema;
	}

//	public String getFullName() {
//		return String.format("`%s`.`%s`", schema, tableName);
//	}

	@Override
	public int getHeight() {
		return 1;
	}




	@Override
	public String toString(int level) {
		StringBuilder sb = new StringBuilder();
		String tabTittle = ToStringUtil.getTab(level);
		String tabContent = ToStringUtil.getTab(level + 1);
		if (this.getAlias() != null) {
			ToStringUtil.appendln(sb, tabTittle + "Query from " + this.getTableName() + "<" + this.getSubAlias() + ">" + " as "
					+ this.getAlias());
		} else {
			ToStringUtil.appendln(sb, tabTittle + "Query from " + this.getTableName() + "<" + this.getSubAlias() + ">");
		}
		ToStringUtil.appendln(sb, tabContent + "isDistinct: " + isDistinct());
		ToStringUtil.appendln(sb, tabContent + "columns: " + ToStringUtil.itemListString(columnsSelected));
		ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
		ToStringUtil.appendln(sb, tabContent + "having: " + ToStringUtil.itemString(havingFilter));
		ToStringUtil.appendln(sb, tabContent + "groupBy: " + ToStringUtil.orderListString(groups));
		ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
		if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
			ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
			ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
		}
		ToStringUtil.appendln(sb, tabContent + "sql: " + this.getSql());
		return sb.toString();
	}
	
	public List<SQLHint> getHintList() {
		return hintList;
	}

	public void setHintList(List<SQLHint> hintList) {
		this.hintList = hintList;
	}

}
