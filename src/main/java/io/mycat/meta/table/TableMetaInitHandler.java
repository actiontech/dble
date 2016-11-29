package io.mycat.meta.table;

import io.mycat.MycatServer;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;

public class TableMetaInitHandler extends AbstractTableMetaHandler {
	private MultiTableMetaHandler multiTableMetaHandler;
	public TableMetaInitHandler(MultiTableMetaHandler multiTableMetaHandler, String schema, TableConfig tbConfig) {
		super(schema, tbConfig);
		this.multiTableMetaHandler = multiTableMetaHandler;
	}

	@Override
	protected void countdown() {
		multiTableMetaHandler.countDown();
	}

	@Override
	protected void handlerTable(TableMeta tableMeta) {
		MycatServer.getInstance().getTmManager().addTable(schema, tableMeta);
	}

}
