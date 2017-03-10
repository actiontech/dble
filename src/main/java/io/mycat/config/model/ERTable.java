package io.mycat.config.model;

public class ERTable {
	private final String table;
	private final String column;
	private final String schema;

	public ERTable(String schema, String table, String column) {
		if (schema == null)
			throw new IllegalArgumentException("ERTable's schema can't be null");
		this.schema = schema;
		if (table == null)
			throw new IllegalArgumentException("ERTable's tableName can't be null");
		this.table = table;
		if (column == null)
			throw new IllegalArgumentException("ERTable's column can't be null");
		this.column = column;
	}

	public String getTable() {
		return table;
	}

	public String getColumn() {
		return column;
	}

	public String getSchema() {
		return schema;
	}

	@Override
	public int hashCode() {
		final int constant = 37;
		int hash = 17;
		hash += constant * (schema == null ? 0 : schema.toLowerCase().hashCode());
		hash += constant * (table == null ? 0 : table.toLowerCase().hashCode());
		hash += constant * (column == null ? 0 : column.toLowerCase().hashCode());
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof ERTable) {
			ERTable erTable = (ERTable) obj;
			return this.schema.equalsIgnoreCase(erTable.getSchema())
					&& this.table.equalsIgnoreCase(erTable.getTable())
					&& this.column.equalsIgnoreCase(erTable.getColumn());
		}
		return false;
	}
}
