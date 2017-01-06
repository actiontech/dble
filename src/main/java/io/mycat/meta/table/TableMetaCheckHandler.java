package io.mycat.meta.table;

import io.mycat.MycatServer;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.SchemaMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;

public class TableMetaCheckHandler extends AbstractTableMetaHandler {

	public TableMetaCheckHandler(String schema, TableConfig tbConfig) {
		super(schema, tbConfig);
	}

	@Override
	protected void countdown() {
	}

	@Override
	protected void handlerTable(TableMeta tableMeta) {
		if(isTableModify(schema,tableMeta)){
			LOGGER.warn("Table [" + tableMeta.getTableName() + "] are modified by other,Please Check IT!");
		}
	}
	private boolean isTableModify(String schema, TableMeta tm){
		String tbName = tm.getTableName();
		SchemaMeta schemaMeta = MycatServer.getInstance().getTmManager().getSchema(schema);
		if (schemaMeta == null) {
			//the DDL may drop database;
			return false;
		}
		TableMeta oldTm = schemaMeta.getTableMeta(tbName);
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
		if (!oldTm.equals(tblMetaTmp) && oldTm.equals(schemaMeta.getTableMeta(tbName))) {
			return true;
		}
		return false;
	}
}
