package io.mycat.meta;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.table.TableMetaHandler;

/**
 * 表结构结果处理
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:09:03 2016/5/11
 */
public class MySQLTableStructureDetector implements Runnable {
	@Override
	public void run() {
		for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) {
			for (TableConfig table : schema.getTables().values()) {
				TableMetaHandler tableMeta = new TableMetaHandler(null, schema.getName(), table);
				tableMeta.execute();
			}
		}
	}
}
