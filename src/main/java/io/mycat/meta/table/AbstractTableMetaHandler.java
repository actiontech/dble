package io.mycat.meta.table;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;

public abstract class AbstractTableMetaHandler {
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaHandler.class);
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLMS = new String[]{
            "Table",
            "Create Table"};
    private static final String sqlPrefix = "show create table ";

	
	private TableConfig tbConfig;
	private AtomicInteger nodesNumber;
	protected String schema;
	public AbstractTableMetaHandler( String schema,  TableConfig tbConfig){
		this.tbConfig = tbConfig;
		this.nodesNumber = new AtomicInteger(tbConfig.getDataNodes().size());
		this.schema = schema;
	}
	public void execute(){
		for (String dataNode : tbConfig.getDataNodes()) {
			try {
				tbConfig.getReentrantReadWriteLock().writeLock().lock();
				ConcurrentMap<String, List<String>> map = new ConcurrentHashMap<>();
				tbConfig.setDataNodeTableStructureSQLMap(map);
			} finally {
				tbConfig.getReentrantReadWriteLock().writeLock().unlock();
			}
			OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLMS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis()));
			resultHandler.setMark("Table Structure");
			PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(dataNode);
			SQLJob sqlJob = new SQLJob(sqlPrefix + tbConfig.getName(), dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
			sqlJob.run();
		}
	}
	protected abstract void countdown();
	protected abstract void handlerTable(TableMeta tableMeta);
	private class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
		private String dataNode;
		private long version;
		public MySQLTableStructureListener(String dataNode, long version) {
			this.dataNode = dataNode;
			this.version = version;
		}

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            try {
            	tbConfig.getReentrantReadWriteLock().writeLock().lock();
                if (!result.isSuccess()) { 
                	//not thread safe
                	LOGGER.warn("Can't get table " + tbConfig.getName() + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!"); 
                	if (nodesNumber.decrementAndGet() == 0) {
                		countdown();
                	}
                    return;
                }
                String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLMS[1]);
                Map<String, List<String>> dataNodeTableStructureSQLMap = tbConfig.getDataNodeTableStructureSQLMap();
                if (dataNodeTableStructureSQLMap.containsKey(currentSql)) {
                    List<String> dataNodeList = dataNodeTableStructureSQLMap.get(currentSql);
                    dataNodeList.add(dataNode);
                } else {
                    List<String> dataNodeList = new LinkedList<>();
                    dataNodeList.add(dataNode);
                    dataNodeTableStructureSQLMap.put(currentSql,dataNodeList);
                }

				if (nodesNumber.decrementAndGet() == 0) {
					TableMeta tableMeta = null;
					if (dataNodeTableStructureSQLMap.size() > 1) {
						// Through the SQL is different, the table Structure may still same.
						// for example: autoIncreament number
						Set<TableMeta> tableMetas = new HashSet<TableMeta>(); 
						for (String sql : dataNodeTableStructureSQLMap.keySet()) {
							tableMeta = initTableMeta(tbConfig.getName(), sql, version);
							tableMetas.add(tableMeta);
						}
						if (tableMetas.size() > 1) {
							consistentWarning(dataNodeTableStructureSQLMap);
						}
						tableMetas.clear();
					} else {
						tableMeta = initTableMeta(tbConfig.getName(), currentSql, version);
					}
					handlerTable(tableMeta);
					countdown();
				}
            } finally {
            	tbConfig.getReentrantReadWriteLock().writeLock().unlock();
            }
        }

        private void consistentWarning(Map<String, List<String>> dataNodeTableStructureSQLMap){
        	LOGGER.warn("Table [" + tbConfig.getName() + "] structure are not consistent!");
            LOGGER.warn("Currently detected: ");
            for(String sql : dataNodeTableStructureSQLMap.keySet()){
                StringBuilder stringBuilder = new StringBuilder();
                for(String dn : dataNodeTableStructureSQLMap.get(sql)){
                    stringBuilder.append("DataNode:[").append(dn).append("]");
                }
                stringBuilder.append(":").append(sql);
                LOGGER.warn(stringBuilder.toString());
            }
        }

		private TableMeta initTableMeta(String table, String sql, long timeStamp) {
			SQLStatementParser parser = new MySqlStatementParser(sql);
			SQLCreateTableStatement createStment = parser.parseCreateTable();
			return MetaHelper.initTableMeta(table, createStment, timeStamp);
		}
    }
}
