package io.mycat.meta.table;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.IndexMeta;
import io.mycat.meta.MySQLTableStructureDetector;
import io.mycat.meta.protocol.MyCatMeta.ColumnMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;

public class TableMetaHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLTableStructureDetector.class);
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLMS = new String[]{
            "Table",
            "Create Table"};
    private static final String sqlPrefix = "show create table ";

	private MultiTableMetaHandler multiTableMetaHandler;
	private TableConfig tbConfig;
	private AtomicInteger nodesNumber;
	private TableMeta.Builder tmBuilder;
	private String schema;
	public TableMetaHandler(MultiTableMetaHandler multiTableMetaHandler, String schema,  TableConfig tbConfig){
		this.multiTableMetaHandler = multiTableMetaHandler;
		this.tbConfig = tbConfig;
		this.nodesNumber = new AtomicInteger(tbConfig.getDataNodes().size());
		this.tmBuilder = TableMeta.newBuilder();
		tmBuilder.setCatalog(schema).setTableName(tbConfig.getName());
		this.schema = schema;
	}
	public void execute(){
		for (String dataNode : tbConfig.getDataNodes()) {
			try {
				tbConfig.getReentrantReadWriteLock().writeLock().lock();
				ConcurrentHashMap<String, List<String>> map = new ConcurrentHashMap<>();
				tbConfig.setDataNodeTableStructureSQLMap(map);
			} finally {
				tbConfig.getReentrantReadWriteLock().writeLock().unlock();
			}
			OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLMS, new MySQLTableStructureListener(dataNode));
			resultHandler.setMark("Table Structure");
			PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(dataNode);
			SQLJob sqlJob = new SQLJob(sqlPrefix + tbConfig.getName(), dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
			sqlJob.run();
		}
	}
	private  class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
		private String dataNode;

		public MySQLTableStructureListener(String dataNode) {
			this.dataNode = dataNode;
		}

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            try {
            	tbConfig.getReentrantReadWriteLock().writeLock().lock();
                if (!result.isSuccess()) {
                    LOGGER.warn("Can't get table " + tbConfig.getName() + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!");
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
					if (dataNodeTableStructureSQLMap.size() > 1) {
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
					List<IndexMeta> indexMetas = new ArrayList<IndexMeta>();
					initTableMeta(tbConfig.getName(),currentSql, tmBuilder, indexMetas);
	    			tmBuilder.setVersion(System.currentTimeMillis());
	    			MycatServer.getInstance().getTmManager().addTable(schema, tmBuilder.build());
	    			for(IndexMeta indexMeta: indexMetas){
	    				MycatServer.getInstance().getTmManager().addIndex(schema, indexMeta);
	    			}
					if(multiTableMetaHandler != null){
						multiTableMetaHandler.countDown();
					}
				}
            } finally {
            	tbConfig.getReentrantReadWriteLock().writeLock().unlock();
            }
        }

		private void initTableMeta(String table, String sql, TableMeta.Builder tmBuilder ,List<IndexMeta> indexMetas) {
    		SQLStatementParser parser = new MySqlStatementParser(sql);
    		SQLCreateTableStatement createStment = parser.parseCreateTable();
    		for (SQLTableElement tableElement : createStment.getTableElementList()) {
    			if (tableElement instanceof SQLColumnDefinition) {
    				ColumnMeta.Builder cmBuilder = ColumnMeta.newBuilder();
    				SQLColumnDefinition column = (SQLColumnDefinition) tableElement;
    				cmBuilder.setTableName(table);
    				cmBuilder.setName(column.getName().getSimpleName());
    				for (SQLColumnConstraint constraint : column.getConstraints()) {
    					if (constraint instanceof SQLNotNullConstraint) {
    						cmBuilder.setCanNull(false);
    					} else if (constraint instanceof SQLNullConstraint) {
    						cmBuilder.setCanNull(true);
    					} else if (constraint instanceof SQLColumnPrimaryKey) {
    						cmBuilder.setKey("PRI");
    					} else if (constraint instanceof SQLColumnUniqueKey) {
    						cmBuilder.setKey("UNI");
    					} else {
    						// ignore 
    					}
    				}
    				if (column.getDefaultExpr() != null) {
    					StringBuilder builder = new StringBuilder();
    					MySqlOutputVisitor visitor = new MySqlOutputVisitor(builder);
    					column.getDefaultExpr().accept(visitor);
    					cmBuilder.setSdefault(builder.toString());
    				}
    				if (column.isAutoIncrement()) {
    					cmBuilder.setAutoIncre(true);
    					tmBuilder.setAiColPos(tmBuilder.getAllColumnsCount());
    				}
    				tmBuilder.addAllColumns(cmBuilder.build());
    			} else if (tableElement instanceof MySqlPrimaryKey) {
    				MySqlPrimaryKey primaryKey = (MySqlPrimaryKey)tableElement; 
    				List<String> columns = new ArrayList<String>();
    				for (int i = 0; i < primaryKey.getColumns().size(); i++) {
    					SQLIdentifierExpr column  = (SQLIdentifierExpr)primaryKey.getColumns().get(i);
    					columns.add(column.getName());
    				}
    				indexMetas.add(new IndexMeta("PRIMARY", table, "PRI", columns));
    			} else if (tableElement instanceof MySqlUnique) {
    				MySqlUnique unique = (MySqlUnique)tableElement;
    				List<String> columns = new ArrayList<String>();
    				for (int i = 0; i < unique.getColumns().size(); i++) {
    					SQLIdentifierExpr column  = (SQLIdentifierExpr)unique.getColumns().get(i);
    					columns.add(column.getName());
    				}
    				indexMetas.add(new IndexMeta(unique.getName().getSimpleName(), table, "UNI", columns));
    			} else if (tableElement instanceof MySqlTableIndex) {
    				MySqlTableIndex index = (MySqlTableIndex)tableElement;
    				List<String> columns = new ArrayList<String>();
    				for (int i = 0; i < index.getColumns().size(); i++) {
    					SQLIdentifierExpr column  = (SQLIdentifierExpr)index.getColumns().get(i);
    					columns.add(column.getName());
    				}
    				indexMetas.add(new IndexMeta(index.getName().getSimpleName(), table, "MUL", columns)); 
    			} else {
    				// ignore
    			}
    		}
        }
    }
}
