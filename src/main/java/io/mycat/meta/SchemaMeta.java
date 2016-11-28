package io.mycat.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.mycat.meta.protocol.MyCatMeta.TableMeta;

public class SchemaMeta {

	/** <table,tableMeta> */
	private final Map<String, TableMeta> tableMetas;
//	private final Map<String, ViewMeta> viewMetas;
	/** <table+'.'+indexName,IndexMeta> */
	private final Map<String, IndexMeta> indexMetas;

	public SchemaMeta() {
		this.tableMetas = new ConcurrentHashMap<String, TableMeta>();
//		this.viewMetas = new ConcurrentHashMap<String, ViewMeta>();
		this.indexMetas = new ConcurrentHashMap<String, IndexMeta>();
	}

	public Map<String, TableMeta> getTableMetas() {
		return tableMetas;
	}

//	public Map<String, ViewMeta> getViewMetas() {
//		return viewMetas;
//	}

	public void addTableMeta(String tbName, TableMeta tblMeta) {
		this.tableMetas.put(tbName, tblMeta);
	}

	public TableMeta dropTable(String tbName) {
		return this.tableMetas.remove(tbName);
	}

	public TableMeta getTableMeta(String tbName) {
		return this.tableMetas.get(tbName);
	}

//	public boolean containsView(String viewName) {
//		return this.viewMetas.containsKey(viewName);
//	}
//
//	public void addViewMeta(String viewName, ViewMeta viewMeta) {
//		this.viewMetas.put(viewName, viewMeta);
//	}
//
//	public void dropView(String viewName) {
//		this.viewMetas.remove(viewName);
//	}

//	public ViewMeta getViewMeta(String viewName) {
//		return this.viewMetas.get(viewName);
//	}

	public void addIndexMeta(String name, IndexMeta indexMeta) {
		this.indexMetas.put(name, indexMeta);
	}

	public IndexMeta getIndexMeta(String name) {
		return this.indexMetas.get(name);
	}

	public List<String> getTables() {
		List<String> tbList = new ArrayList<String>();
		tbList.addAll(tableMetas.keySet());
//		tbList.addAll(viewMetas.keySet());
		return tbList;
	}
}
