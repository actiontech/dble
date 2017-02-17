package io.mycat.plan;

import org.apache.commons.lang.StringUtils;

public class NamedField {
	public String table;
	public String name;
	// 这个field隶属于哪个节点
	public final PlanNode planNode;

	public NamedField(PlanNode tableNode) {
		this(null, null, tableNode);
	}

	public NamedField(String table, String name, PlanNode planNode) {
		this.table = table;
		this.name = name;
		this.planNode = planNode;
	}

	@Override
	public int hashCode() {
		int prime = 2;
		int hashCode = table == null ? 0 : table.hashCode();
		hashCode = hashCode * prime + (name == null ? 0 : name.toLowerCase().hashCode());
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof NamedField))
			return false;
		NamedField other = (NamedField) obj;
		if (StringUtils.equals(table, other.table) && StringUtils.equalsIgnoreCase(name, other.name))
			return true;
		else
			return false;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("table:").append(table).append(",name:").append(name).toString();
	}
}
