package io.mycat.plan.node.view;

import java.util.ArrayList;
import java.util.List;

import io.mycat.meta.protocol.StructureMeta.ColumnMeta;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.util.ToStringUtil;

/**
 * present a view
 * 
 * @author ActionTech
 * 
 */
public class ViewNode extends QueryNode {
	public PlanNodeType type() {
		return PlanNodeType.VIEW;
	}

	private String catalog;
	private String viewname;
	private String createSql;

	public ViewNode(String catalog, String viewName, PlanNode selNode, String createSql) {
		super(selNode);
		this.catalog = catalog;
		this.viewname = viewName;
		this.createSql = createSql;
		this.getChild().setAlias(viewname);
	}

	public QueryNode toQueryNode() {
		QueryNode newNode = new QueryNode((PlanNode) this.getChild().copy());
		this.copySelfTo(newNode);
		return newNode;
	}

	public String getCatalog() {
		return catalog;
	}

	/**
	 * 根据编译好的viewnode获取当前view的列
	 * 
	 * @return
	 */
	public List<ColumnMeta> getColumns() {
		List<ColumnMeta> cols = new ArrayList<ColumnMeta>();
		for (Item sel : getColumnsSelected()) {
			String cn = sel.getAlias() == null ? sel.getItemName() : sel.getAlias();
			ColumnMeta.Builder cmBuilder = ColumnMeta.newBuilder();
			cmBuilder.setName(cn);
			cols.add(cmBuilder.build());
		}
		return cols;
	}

	/**
	 * 获得该view应用到的table名
	 * 
	 * @return
	 */
	public List<String> getReferedTables() {
		List<String> tls = new ArrayList<String>();
		for (TableNode tn : this.getReferedTableNodes()) {
			tls.add(tn.getTableName());
		}
		return tls;
	}

	public String getCreateSql() {
		return createSql;
	}

	@Override
	public ViewNode copy() {
		PlanNode selCopy = (PlanNode) this.getChild().copy();
		return new ViewNode(catalog, viewname, selCopy, createSql);
	}

	@Override
	public String toString(int level) {
		StringBuilder sb = new StringBuilder();
		String tabTittle = ToStringUtil.getTab(level);
		ToStringUtil.appendln(sb, tabTittle + "View  " + this.viewname + " AS: ");
		ToStringUtil.appendln(sb, this.getChild().toString(level + 1));
		return sb.toString();
	}

}
