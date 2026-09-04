/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.ItemString;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncIsnull;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.optimizer.FilterPusher;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ATK-4732: OR-split must keep withIsNull so LOJ right-side push is refused.
 */
public class FilterUtilsSplitOrTest {

    @Test
    public void splitOrWithIsNullCannotPushToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("my_payee_lists");
        TableNode right = newBareTable("payee_extra_info");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField col = fieldOn(right, "i", "is_other");
        ItemFuncIsnull isNull = new ItemFuncIsnull(col);
        isNull.getReferTables().add(right);
        ItemFuncEqual eq = new ItemFuncEqual(col, new ItemString("N"));
        eq.getReferTables().add(right);

        ItemCondOr original = new ItemCondOr(Arrays.asList(isNull, eq));
        PlanUtil.refreshReferTables(original);
        Assert.assertTrue(original.isWithIsNull());

        List<Item> splits = FilterUtils.splitFilter(original);
        ItemCondOr extracted = oneExtractedOr(splits, original);
        Assert.assertTrue("extracted OR must keep withIsNull", extracted.isWithIsNull());
        Assert.assertFalse(PlanUtil.canPush(extracted, right, join));
        Assert.assertFalse(PlanUtil.canPush(original, right, join));

        Assert.assertTrue(PlanUtil.canPush(eq, right, join));
        Assert.assertFalse(PlanUtil.canPush(isNull, right, join));
    }

    @Test
    public void splitOrWithoutIsNullCanStillPushToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("t");
        TableNode right = newBareTable("i");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField col = fieldOn(right, "i", "is_other");
        ItemFuncEqual eqN = new ItemFuncEqual(col, new ItemString("N"));
        eqN.getReferTables().add(right);
        ItemFuncEqual eqY = new ItemFuncEqual(col, new ItemString("Y"));
        eqY.getReferTables().add(right);

        ItemCondOr valueOr = new ItemCondOr(Arrays.asList(eqN, eqY));
        PlanUtil.refreshReferTables(valueOr);
        Assert.assertFalse(valueOr.isWithIsNull());

        List<Item> splits = FilterUtils.splitFilter(valueOr);
        ItemCondOr extracted = oneExtractedOr(splits, valueOr);
        Assert.assertFalse(extracted.isWithIsNull());
        Assert.assertTrue(PlanUtil.canPush(extracted, right, join));
    }

    @Test
    public void splitAndOrWithIsNullCannotPushToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("my_payee_lists");
        TableNode right = newBareTable("payee_extra_info");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField colA = fieldOn(right, "i", "is_other");
        ItemField colB = fieldOn(right, "i", "list_id");
        ItemFuncIsnull isNull = new ItemFuncIsnull(colA);
        isNull.getReferTables().add(right);
        ItemFuncEqual eqB = new ItemFuncEqual(colB, new ItemString("PID"));
        eqB.getReferTables().add(right);
        ItemFuncEqual eqN = new ItemFuncEqual(colA, new ItemString("N"));
        eqN.getReferTables().add(right);

        ItemCondAnd firstBranch = new ItemCondAnd(Arrays.asList(isNull, eqB));
        PlanUtil.refreshReferTables(firstBranch);
        ItemCondOr original = new ItemCondOr(Arrays.asList(firstBranch, eqN));
        PlanUtil.refreshReferTables(original);
        Assert.assertTrue(original.isWithIsNull());

        List<Item> splits = FilterUtils.splitFilter(original);
        ItemCondOr extracted = oneExtractedOr(splits, original);
        Assert.assertTrue("AND-wrapped IS NULL must roll withIsNull onto extracted OR",
                extracted.isWithIsNull());
        Assert.assertFalse(PlanUtil.canPush(extracted, right, join));
        Assert.assertEquals(1, extracted.getReferTables().size());
        Assert.assertTrue(extracted.getReferTables().contains(right));
    }

    @Test
    public void filterPusherDoesNotPushIsNullOrToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("my_payee_lists");
        TableNode right = newBareTable("payee_extra_info");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField col = fieldOn(right, "i", "is_other");
        ItemFuncIsnull isNull = new ItemFuncIsnull(col);
        isNull.getReferTables().add(right);
        ItemFuncEqual eq = new ItemFuncEqual(col, new ItemString("N"));
        eq.getReferTables().add(right);
        ItemCondOr original = new ItemCondOr(Arrays.asList(isNull, eq));
        PlanUtil.refreshReferTables(original);
        wirePushDownColumn(join, right, col);
        join.query(original);

        JoinNode optimized = (JoinNode) FilterPusher.optimize(join);
        Assert.assertNull("IS NULL OR must stay off the LOJ right leaf",
                optimized.getRightNode().getWhereFilter());
        Assert.assertNotNull(optimized.getWhereFilter());
        Assert.assertTrue(optimized.getWhereFilter().isWithIsNull());
    }

    @Test
    public void filterPusherDoesNotPushAndOrWithIsNullToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("my_payee_lists");
        TableNode right = newBareTable("payee_extra_info");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField colA = fieldOn(right, "i", "is_other");
        ItemField colB = fieldOn(right, "i", "list_id");
        ItemFuncIsnull isNull = new ItemFuncIsnull(colA);
        isNull.getReferTables().add(right);
        ItemFuncEqual eqB = new ItemFuncEqual(colB, new ItemString("PID"));
        eqB.getReferTables().add(right);
        ItemFuncEqual eqN = new ItemFuncEqual(colA, new ItemString("N"));
        eqN.getReferTables().add(right);

        ItemCondAnd firstBranch = new ItemCondAnd(Arrays.asList(isNull, eqB));
        PlanUtil.refreshReferTables(firstBranch);
        ItemCondOr original = new ItemCondOr(Arrays.asList(firstBranch, eqN));
        PlanUtil.refreshReferTables(original);
        wirePushDownColumn(join, right, colA);
        wirePushDownColumn(join, right, colB);
        join.query(original);

        JoinNode optimized = (JoinNode) FilterPusher.optimize(join);
        Assert.assertNull("AND-wrapped IS NULL OR must stay off the LOJ right leaf",
                optimized.getRightNode().getWhereFilter());
        Assert.assertNotNull(optimized.getWhereFilter());
        Assert.assertTrue(optimized.getWhereFilter().isWithIsNull());
    }

    @Test
    public void filterPusherStillPushesValueOrToLeftJoinRight() throws Exception {
        TableNode left = newBareTable("t");
        TableNode right = newBareTable("i");
        JoinNode join = new JoinNode(left, right);
        join.setLeftOuterJoin();

        ItemField col = fieldOn(right, "i", "is_other");
        ItemFuncEqual eqN = new ItemFuncEqual(col, new ItemString("N"));
        eqN.getReferTables().add(right);
        ItemFuncEqual eqY = new ItemFuncEqual(col, new ItemString("Y"));
        eqY.getReferTables().add(right);
        ItemCondOr valueOr = new ItemCondOr(Arrays.asList(eqN, eqY));
        PlanUtil.refreshReferTables(valueOr);
        Assert.assertFalse(valueOr.isWithIsNull());
        wirePushDownColumn(join, right, col);
        join.query(valueOr);

        JoinNode optimized = (JoinNode) FilterPusher.optimize(join);
        Assert.assertNotNull("value OR must reach the LOJ right leaf so the no-push cases are not vacuously green",
                optimized.getRightNode().getWhereFilter());
        Assert.assertFalse(optimized.getRightNode().getWhereFilter().isWithIsNull());
    }

    /**
     * FilterPusher.refreshPdFilters / PlanUtil.pushDownCol look up the column
     * on JoinNode.innerFields then the child's outerFields. Bare TableNode
     * fixtures have neither, so a real right-side push NPEs and a "did not
     * push" assertion can pass without ever attempting the rewrite.
     */
    private static void wirePushDownColumn(JoinNode join, TableNode leaf, ItemField col) {
        NamedField childField = new NamedField(col.getDbName(), col.getTableName(), col.getItemName(), leaf);
        leaf.getOuterFields().put(childField, col);
        NamedField joinField = new NamedField(col.getDbName(), col.getTableName(), col.getItemName(), leaf);
        join.getInnerFields().put(joinField, childField);
    }

    /**
     * splitFilter keeps the original OR and also adds a newly built per-table OR.
     * Tests must assert on that new instance; the original already has withIsNull.
     */
    private static ItemCondOr oneExtractedOr(List<Item> splits, ItemCondOr original) {
        boolean keptOriginal = false;
        List<ItemCondOr> extracted = new ArrayList<ItemCondOr>();
        for (Item item : splits) {
            if (item == original) {
                keptOriginal = true;
            } else if (item instanceof ItemCondOr) {
                extracted.add((ItemCondOr) item);
            }
        }
        Assert.assertTrue("original OR must stay in the split list", keptOriginal);
        Assert.assertEquals("groupByReferTable must emit exactly one extracted OR", 1, extracted.size());
        return extracted.get(0);
    }

    private static ItemField fieldOn(TableNode table, String alias, String column) {
        ItemField col = new ItemField(null, alias, column);
        col.getReferTables().add(table);
        return col;
    }

    private static TableNode newBareTable(String name) throws Exception {
        Constructor<TableNode> ctor = TableNode.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        TableNode node = ctor.newInstance();
        node.setTableName(name);
        return node;
    }
}
