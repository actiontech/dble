package io.mycat.meta;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.table.AbstractTableMetaHandler;
import io.mycat.meta.table.TableMetaCheckHandler;


public class MySQLTableStructureCheck implements Runnable {
	@Override
	public void run() {
		for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) {
			if(!MycatServer.getInstance().getTmManager().checkDbExists(schema.getName()) ){
				continue;
			}
			for (TableConfig table : schema.getTables().values()) {
				if(!MycatServer.getInstance().getTmManager().checkTableExists(schema.getName(), table.getName())) {
					continue;
				}
				AbstractTableMetaHandler handler = new TableMetaCheckHandler(schema.getName(), table);
				handler.execute();
			}
		}
	}
}
