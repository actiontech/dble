package io.mycat.meta.table;

import io.mycat.MycatServer;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.ProxyMetaManager;
import io.mycat.meta.protocol.StructureMeta.TableMeta;

import java.util.Set;

public class TableMetaCheckHandler extends AbstractTableMetaHandler {
	private final ProxyMetaManager tmManager;
	public TableMetaCheckHandler(ProxyMetaManager tmManager,String schema, TableConfig tbConfig, Set<String> selfNode) {
		super(schema, tbConfig, selfNode);
		this.tmManager = tmManager;
	}

	@Override
	protected void countdown() {
	}

	@Override
	protected void handlerTable(TableMeta tableMeta) {
		if(isTableModify(schema,tableMeta)){
			LOGGER.warn("Table [" + tableMeta.getTableName() + "] are modified by other,Please Check IT!");
		}
		LOGGER.debug("checking table Table [" + tableMeta.getTableName() + "]");
	}
	private boolean isTableModify(String schema, TableMeta tm){
		String tbName = tm.getTableName();
		TableMeta oldTm = tmManager.getSyncTableMeta(schema, tbName);
		if(oldTm == null ){
			//the DDL may drop table;
			return false;
		}
		if(oldTm.getVersion()>=tm.getVersion()){
			//there is an new version TableMeta after check start
			return false;
		}
		TableMeta tblMetaTmp = tm.toBuilder().setVersion(oldTm.getVersion()).build();
		//TODO: thread not safe
		if (!oldTm.equals(tblMetaTmp) && oldTm.equals(tmManager.getSyncTableMeta(schema, tbName))) {
			return true;
		}
		return false;
	}
}
