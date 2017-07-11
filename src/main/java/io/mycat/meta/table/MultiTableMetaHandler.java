package io.mycat.meta.table;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;

public class MultiTableMetaHandler {
	private AtomicInteger tableNumbers;
	private String schema;
	private SchemaConfig config;
	private SchemaMetaHandler schemaMetaHandler;
	private Set<String> selfNode;
	public MultiTableMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig config, Set<String> selfNode) {
		this.schemaMetaHandler = schemaMetaHandler;
		this.config = config;
		this.schema = config.getName();
		this.selfNode = selfNode;
		tableNumbers = new AtomicInteger(config.getTables().size());
	}

	public void execute() {
		MycatServer.getInstance().getTmManager().createDatabase(schema);
		if (config.getTables().size() == 0) {
			schemaMetaHandler.countDown();
			return;
		}
		for (Entry<String, TableConfig> entry : config.getTables().entrySet()) {
			AbstractTableMetaHandler table = new TableMetaInitHandler(this, schema, entry.getValue(), selfNode);
			table.execute();
		}

	}

	public void countDown() {
		if (tableNumbers.decrementAndGet() == 0)
			schemaMetaHandler.countDown();
	}
}
