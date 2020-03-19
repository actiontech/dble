/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLConfigLoader;
import com.actiontech.dble.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.config.model.*;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.optimizer.ERJoinChooser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfigTest {

    private SystemConfig system;
    private final Map<String, UserConfig> users;
    private Map<String, PhysicalDataHost> dataHosts;
    private Map<ERTable, Set<ERTable>> erRealtions;

    public ConfigTest() {

        String schemaFile = "/config/schema.xml";
        String ruleFile = "/config/rule.xml";

        XMLSchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile, true, null);
        XMLConfigLoader configLoader = new XMLConfigLoader();

        this.system = configLoader.getSystemConfig();
        this.users = configLoader.getUserConfigs();
        this.dataHosts = initDataHosts(schemaLoader);
        this.erRealtions = schemaLoader.getErRelations();

    }

    /**
     * test FKERmap
     * <p>
     * <schema name="dbtest">
     * <table name="tb1" dataNode="dnTest2,dnTest1" rule="rule1" />
     * <table name="tb2" dataNode="dnTest2,dnTest3" rule="rule1" />
     * <table name="tb3" dataNode="dnTest1,dnTest2" rule="rule1" />
     * </schema>
     * <schema name="ertest">
     * <table name="er_parent" cacheKey="ID" dataNode="dnTest1,dnTest2" rule="rule1">
     * <childTable name="er_child1" cacheKey="child1_id" joinKey="child1_id" parentKey="id" >
     * <childTable name="er_grandson" cacheKey="grandson_id" joinKey="grandson_id" parentKey="child1_id" />
     * </childTable>
     * <childTable name="er_child2" cacheKey="child2_id" joinKey="child2_id" parentKey="id2" />
     * <childTable name="er_child3" cacheKey="child3_id" joinKey="child_char" parentKey="c_char" />
     * <childTable name="er_child4" cacheKey="child4_id" joinKey="child4_id" parentKey="id2" >
     * <childTable name="er_grandson2" cacheKey="grandson2_id" joinKey="grandson2_id" parentKey="child4_id2" />
     * </childTable>
     * <childTable name="er_child5" cacheKey="child5_id" joinKey="child5_id" parentKey="id" >
     * <childTable name="er_grandson3" cacheKey="grandson3_id" joinKey="grandson3_id" parentKey="child5_id2" />
     * </childTable>
     * </table>
     * </schema>
     */
    @Test
    public void testErRelations() {
        String schemaName1 = "dbtest";
        String schemaName2 = "ertest";
        ERTable er_parent0 = new ERTable(schemaName2, "er_parent", "ID");
        ERTable er_dbtest_tb1 = new ERTable(schemaName1, "tb1", "ID");
        Assert.assertFalse(testERjoin(er_parent0, er_dbtest_tb1));
        Assert.assertFalse(testERjoin(er_dbtest_tb1, er_parent0));
        ERTable er_dbtest_tb2 = new ERTable(schemaName1, "tb2", "ID");
        Assert.assertFalse(testERjoin(er_parent0, er_dbtest_tb2));
        Assert.assertFalse(testERjoin(er_dbtest_tb2, er_parent0));
        ERTable er_dbtest_tb3 = new ERTable(schemaName1, "tb3", "ID");
        Assert.assertTrue(testSymmetryERJoin(er_parent0, er_dbtest_tb3));
        ERTable er_child1 = new ERTable(schemaName2, "er_child1", "child1_id");
        ERTable er_grandson = new ERTable(schemaName2, "er_grandson", "grandson_id");
        Assert.assertTrue(testSymmetryERJoin(er_parent0, er_child1));
        Assert.assertTrue(testSymmetryERJoin(er_parent0, er_grandson));
        Assert.assertTrue(testSymmetryERJoin(er_child1, er_grandson));


        ERTable er_parent1 = new ERTable(schemaName2, "er_parent", "ID2");
        ERTable er_child2 = new ERTable(schemaName2, "er_child2", "CHILD2_ID");
        Assert.assertTrue(testSymmetryERJoin(er_parent1, er_child2));
        ERTable er_child4 = new ERTable(schemaName2, "er_child4", "CHILD4_ID");
        Assert.assertTrue(testSymmetryERJoin(er_parent1, er_child4));
        ERTable er_child4_2 = new ERTable(schemaName2, "er_child4", "child4_id2");
        ERTable er_grandson2 = new ERTable(schemaName2, "er_grandson2", "grandson2_id");
        Assert.assertTrue(testSymmetryERJoin(er_child4_2, er_grandson2));
        ERTable er_parent2 = new ERTable(schemaName2, "er_parent", "c_char");
        ERTable er_child3 = new ERTable(schemaName2, "er_child3", "child_char");
        Assert.assertTrue(testSymmetryERJoin(er_parent2, er_child3));
        ERTable er_child5 = new ERTable(schemaName2, "er_child5", "child5_id");
        Assert.assertTrue(testSymmetryERJoin(er_parent0, er_child5));
        ERTable er_child5_2 = new ERTable(schemaName2, "er_child5", "child5_id2");
        ERTable er_grandson3 = new ERTable(schemaName2, "er_grandson3", "grandson3_id");
        Assert.assertTrue(testSymmetryERJoin(er_child5_2, er_grandson3));

        Assert.assertTrue(testSymmetryERJoin(er_child1, er_child5));
        Assert.assertTrue(testSymmetryERJoin(er_grandson, er_child5));

    }

    private boolean testSymmetryERJoin(ERTable er0, ERTable er1) {
        return testERjoin(er0, er1) && testERjoin(er1, er0);
    }

    private boolean testERjoin(ERTable er0, ERTable er1) {
        Class<ERJoinChooser> classERJoin = ERJoinChooser.class;
        try {
            Constructor<ERJoinChooser> construtor = classERJoin.getConstructor(new Class[]{JoinNode.class, Map.class});
            Object instance = construtor.newInstance(new Object[]{new JoinNode(), this.erRealtions});
            Method isERRelstionMethod = classERJoin.getDeclaredMethod("isErRelation", new Class[]{ERTable.class, ERTable.class});
            isERRelstionMethod.setAccessible(true);
            Object result = isERRelstionMethod.invoke(instance, new Object[]{er0, er1});
            return (Boolean) result;
        } catch (Exception e) {
            System.out.println(e);
            return false;
        }
    }

    /**
     * testTempReadHostAvailable
     */
    @Test
    public void testTempReadHostAvailable() {
        PhysicalDataHost pool = this.dataHosts.get("localhost2");
        DataHostConfig hostConfig = pool.getWriteSource().getHostConfig();
        Assert.assertTrue(hostConfig.isTempReadHostAvailable() == true);
    }

    


    /**
     * testReadHostWeight
     *
     * @throws Exception
     */
    @Test
    public void testReadHostWeight() throws Exception {

        ArrayList<PhysicalDataSource> okSources = new ArrayList<PhysicalDataSource>();

        PhysicalDataHost pool = this.dataHosts.get("localhost2");
        okSources.addAll(pool.getAllActiveDataSources());
        PhysicalDataSource source = pool.randomSelect(okSources);

        Assert.assertTrue(source != null);
    }

    private Map<String, PhysicalDataHost> initDataHosts(SchemaLoader schemaLoader) {
        Map<String, DataHostConfig> nodeConfs = schemaLoader.getDataHosts();
        Map<String, PhysicalDataHost> nodes = new HashMap<String, PhysicalDataHost>(
                nodeConfs.size());
        for (DataHostConfig conf : nodeConfs.values()) {
            PhysicalDataHost pool = getPhysicalDBPool(conf);
            nodes.put(pool.getHostName(), pool);
        }
        return nodes;
    }

    private PhysicalDataSource createDataSource(DataHostConfig conf,
                                                String hostName, DataSourceConfig node, boolean isRead) {
        node.setIdleTimeout(system.getIdleTimeout());
        return new MySQLDataSource(node, conf, isRead);
    }

    private PhysicalDataHost getPhysicalDBPool(DataHostConfig conf) {
        String name = conf.getName();
        PhysicalDataSource writeSources = createDataSource(conf, name, conf.getWriteHost(), false);
        PhysicalDataSource[] readSources = new PhysicalDataSource[conf.getReadHosts().length];
        int i = 0;
        for (DataSourceConfig readHost : conf.getReadHosts()) {
            readSources[i++] = createDataSource(conf, name, readHost, true);
        }
        PhysicalDataHost pool = new PhysicalDataHost(conf.getName(), conf, writeSources,
                readSources, conf.getBalance());
        return pool;
    }

}
