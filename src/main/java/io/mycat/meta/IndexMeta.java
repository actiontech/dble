package io.mycat.meta;

import java.util.List;

public class IndexMeta {
	public static enum IndexType {
		PRI, UNI, MUL
	}

	private final String name;
	private final String table;
	private final IndexType indexType;

	private List<String> indexColumns;

	public IndexMeta(String name, String table, String type, List<String> columns) {
		this.name = name;
		this.table = table;
		this.indexType = IndexType.valueOf(type);
		if (indexType == null) {
			throw new IllegalArgumentException("unknown index type " + type);
		}
		this.indexColumns = columns;
	}

	public String getName() {
		return name;
	}

	public String getTable() {
		return table;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public boolean indexCanUsed(String column) {
		if (indexColumns.size()>0 && indexColumns.get(0).equals(column)) {
			return true;
		}
		return false;
	}
}
