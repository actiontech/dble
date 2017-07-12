package io.mycat.plan.node;

import java.util.*;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.PlanNode;
import io.mycat.plan.util.ToStringUtil;

/**
 * 匿名表，比如select 1,only exists selecteditems
 * 
 * @author ActionTech
 * 
 */

public class NoNameNode extends PlanNode {

	private final static String NONAME = "";
	private final String catalog;

	public PlanNodeType type() {
		return PlanNodeType.NONAME;
	}

	public NoNameNode(String catalog, String sql) {
		this.catalog = catalog;
		this.sql = sql;
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(catalog);
		this.setNoshardNode(new HashSet<>(Arrays.asList(schema.getMetaDataNode())));
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return NONAME;
	}

	@Override
	public NoNameNode copy() {
		NoNameNode noNameNode = new NoNameNode(catalog, sql);
		this.copySelfTo(noNameNode);
		return noNameNode;
	}

	@Override
	public String getPureName() {
		return this.getTableName();
	}

	@Override
	public List<TableNode> getReferedTableNodes() {
		return new ArrayList<TableNode>();
	}

	public String getCatalog() {
		return this.catalog;
	}

	@Override
	public int getHeight() {
		return 0;
	}

	@Override
	public String toString(int level) {
		StringBuilder sb = new StringBuilder();
		String tabTittle = ToStringUtil.getTab(level);

		if (this.getAlias() != null) {
			ToStringUtil.appendln(sb, tabTittle + "Query from[ " + this.getSql() + " ] as " + this.getAlias());
		} else {
			ToStringUtil.appendln(sb, tabTittle + "Query from[ " + this.getSql() + " ]");
		}
		return sb.toString();
	}
}
