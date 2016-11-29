package io.mycat.meta.table;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;

public class MultiTableMetaHandler {
	private AtomicInteger tableNumbers;
	private String schema;
	private SchemaConfig config;
	private SchemaMetaHandler schemaMetaHandler;

	public MultiTableMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig config) {
		this.schemaMetaHandler = schemaMetaHandler;
		this.config = config;
		this.schema = config.getName();
		tableNumbers = new AtomicInteger(config.getTables().size());
	}

	public void execute() {
		MycatServer.getInstance().getTmManager().createDatabase(schema);
		for (Entry<String, TableConfig> entry : config.getTables().entrySet()) {
			AbstractTableMetaHandler table = new TableMetaInitHandler(this, schema, entry.getValue());
			table.execute();
		}
	}

	public void countDown() {
		if (tableNumbers.decrementAndGet() == 0)
			schemaMetaHandler.countDown();
	}
}
