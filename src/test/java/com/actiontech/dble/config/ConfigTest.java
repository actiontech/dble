/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.loader.xml.XMLDbLoader;
import com.actiontech.dble.config.loader.xml.XMLShardingLoader;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.optimizer.ERJoinChooser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class ConfigTest {

    private Map<ERTable, Set<ERTable>> erRealtions;

    public ConfigTest() {

        String shardingFile = "/config/sharding.xml";
        XMLShardingLoader schemaLoader = new XMLShardingLoader(shardingFile, true, null);
        this.erRealtions = schemaLoader.getErRelations();

    }

    /**
     * test FKERmap
     * <p>
     * <sharding name="dbtest">
     * <table name="tb1" shardingNode="dnTest2,dnTest1" rule="rule1" />
     * <table name="tb2" shardingNode="dnTest2,dnTest3" rule="rule1" />
     * <table name="tb3" shardingNode="dnTest1,dnTest2" rule="rule1" />
     * </sharding>
     * <sharding name="ertest">
     * <table name="er_parent" dataNode="dnTest1,dnTest2" rule="rule1">
     * <childTable name="er_child1" joinColumn="child1_id" parentColumn="id" >
     * <childTable name="er_grandson" joinColumn="grandson_id" parentColumn="child1_id" />
     * </childTable>
     * <childTable name="er_child2" joinColumn="child2_id" parentColumn="id2" />
     * <childTable name="er_child3" joinColumn="child_char" parentColumn="c_char" />
     * <childTable name="er_child4" joinColumn="child4_id" parentColumn="id2" >
     * <childTable name="er_grandson2" joinColumn="grandson2_id" parentColumn="child4_id2" />
     * </childTable>
     * <childTable name="er_child5" joinColumn="child5_id" parentColumn="id" >
     * <childTable name="er_grandson3" joinColumn="grandson3_id" parentColumn="child5_id2" />
     * </childTable>
     * </table>
     * </sharding>
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
            Object instance = construtor.newInstance(new Object[]{new JoinNode(63), this.erRealtions});
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
     * testReadHostWeight
     *
     * @throws Exception
     */
    @Test
    public void testReadHostWeight() throws Exception {
        String dbFile = "/config/db.xml";
        XMLDbLoader dbLoader = new XMLDbLoader(dbFile, null);
        Map<String, PhysicalDbGroup> dbGroups = dbLoader.getDbGroups();
        PhysicalDbGroup pool = dbGroups.get("localhost2");

        PhysicalDbInstance source = pool.select(true);
        Assert.assertNotNull(source);
    }

}
