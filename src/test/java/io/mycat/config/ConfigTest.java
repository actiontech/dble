package io.mycat.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLConfigLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.DataHostConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.UserConfig;
import junit.framework.Assert;

public class ConfigTest {
	
	private SystemConfig system;
	private final Map<String, UserConfig> users;
	private Map<String, SchemaConfig> schemas;
	private Map<String, PhysicalDBPool> dataHosts;	
	
	public ConfigTest() {
		
		String schemaFile = "/config/schema.xml";
		String ruleFile = "/config/rule.xml";
		
		XMLSchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		XMLConfigLoader configLoader = new XMLConfigLoader();
		
		this.system = configLoader.getSystemConfig();
		this.users = configLoader.getUserConfigs();
		this.schemas = schemaLoader.getSchemas();		
        this.dataHosts = initDataHosts(schemaLoader);
        
	}
	
	/**
	 * 测试 临时读可用 配置
	 */
	@Test
	public void testTempReadHostAvailable() {
		PhysicalDBPool pool = this.dataHosts.get("localhost2");   
		DataHostConfig hostConfig = pool.getSource().getHostConfig();
		Assert.assertTrue( hostConfig.isTempReadHostAvailable() == true );
	}
	
	/**
	 * 测试 用户服务降级 拒连 配置
	 */
	@Test
	public void testReadUserBenchmark() {
		UserConfig userConfig = this.users.get("test");
		int benchmark = userConfig.getBenchmark();
		Assert.assertTrue( benchmark == 11111 );
	}
	
	
	/**
     * 测试 读服务的 权重
     *
     * @throws Exception
     */
    @Test
    public void testReadHostWeight() throws Exception {
    	
    	ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>();
    	
    	PhysicalDBPool pool = this.dataHosts.get("localhost2");   
    	okSources.addAll(pool.getAllDataSources());    	
    	PhysicalDatasource source = pool.randomSelect( okSources );
  
    	Assert.assertTrue( source != null );
    }
    
    /**
     * 测试 动态日期表
     *
     * @throws Exception
     */
    @Test
    public void testDynamicYYYYMMTable() throws Exception {
    	SchemaConfig sc = this.schemas.get("dbtest1");
    	Map<String, TableConfig> tbm = sc.getTables();
    	Assert.assertTrue( tbm.size() == 32);    	
    }
    
	private Map<String, PhysicalDBPool> initDataHosts(SchemaLoader schemaLoader) {
		Map<String, DataHostConfig> nodeConfs = schemaLoader.getDataHosts();
		Map<String, PhysicalDBPool> nodes = new HashMap<String, PhysicalDBPool>(
				nodeConfs.size());
		for (DataHostConfig conf : nodeConfs.values()) {
			PhysicalDBPool pool = getPhysicalDBPool(conf);
			nodes.put(pool.getHostName(), pool);
		}
		return nodes;
	}
    private PhysicalDatasource[] createDataSource(DataHostConfig conf,
			String hostName, DBHostConfig[] nodes, boolean isRead) {
		PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i].setIdleTimeout(system.getIdleTimeout());
			MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
			dataSources[i] = ds;
		}
		return dataSources;
	}

	private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf) {
		String name = conf.getName();
		PhysicalDatasource[] writeSources = createDataSource(conf, name, conf.getWriteHosts(), false);
		Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
		Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<Integer, PhysicalDatasource[]>(
				readHostsMap.size());
		for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
			PhysicalDatasource[] readSources = createDataSource(conf, name, entry.getValue(), true);
			readSourcesMap.put(entry.getKey(), readSources);
		}
		PhysicalDBPool pool = new PhysicalDBPool(conf.getName(),conf, writeSources,
				readSourcesMap, conf.getBalance(), conf.getWriteType());
		return pool;
	}

}
