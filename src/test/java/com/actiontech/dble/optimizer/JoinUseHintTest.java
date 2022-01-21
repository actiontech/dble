/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.optimizer;

import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.actiontech.dble.plan.optimizer.HintPlanNodeGroup;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.RouteService;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/**
 * @author dcy
 * Create Date: 2021-12-01
 */
@SuppressWarnings("UnstableApiUsage")
@Ignore
public class JoinUseHintTest extends BaseSqlHintTest {
    private static final Logger LOGGER = LogManager.getLogger(JoinUseHintTest.class);
    Collection<List<String>> wrongCollections = new ArrayList<>();
    Collection<List<String>> successCollections = new ArrayList<>();
    private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
    String sql;

    @Before
    public void setUp() {
    }


    @Test
    public void alias() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join (select * from e )b on a.id=b.id inner join c on b.id=c.id inner join d on c.id=d.id where b.id=1";
        execute(dataSet, sql);
        assertEquals(1 + 3 + 3 + 1, successCollections.size());
        assertEquals("[[a, b, c, d], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, a, c, d]]", successCollections.toString());
    }

    @Test
    public void alias2() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join e as b on a.id=b.id inner join c on b.id=c.id inner join d on c.id=d.id where b.id=1";
        execute(dataSet, sql);
        assertEquals(1 + 3 + 3 + 1, successCollections.size());
        assertEquals("[[a, b, c, d], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, a, c, d]]", successCollections.toString());
    }

    @Test
    public void general() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id inner join d on c.id=d.id where b.id=1";
        execute(dataSet, sql);
        assertEquals(1 + 3 + 3 + 1, successCollections.size());
        assertEquals("[[a, b, c, d], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, a, c, d]]", successCollections.toString());
    }

    @Test
    public void general3() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id  and c.id=a.id , d";
        execute(dataSet, sql);
        assertEquals(dataSet.size(), successCollections.size());
    }

    @Test
    public void general2() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on a.id=c.id inner join d on c.id=d.id where b.id=1";
        execute(dataSet, sql);
        assertEquals(1 + 3 + 3 + 1, successCollections.size());
        assertEquals("[[a, b, c, d], [a, c, d, b], [a, c, b, d], [c, a, b, d], [c, a, d, b], [c, d, a, b], [d, c, a, b], [b, a, c, d]]", successCollections.toString());
    }

    @Test
    public void wrongHint() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id";
        execute(dataSet, sql);
        assertEquals(0, successCollections.size());
    }

    @Test
    public void dupHint() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id";
        execute(dataSet, sql);
        assertEquals(0, successCollections.size());
    }

    @Test
    public void missHint() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id";
        execute(dataSet, sql);
        assertEquals(0, successCollections.size());
    }


    @Test
    public void repeatCondition() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id and a.id=b.id  inner join c on b.id=c.id and b.id=c.id inner join d on c.id=d.id and c.id=d.id ";
        execute(dataSet, sql);
        assertEquals(1 + 3 + 3 + 1, successCollections.size());
        assertEquals("[[a, b, c, d], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, a, c, d]]", successCollections.toString());
    }

    @Test
    public void repeatCondition2() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a left join b on a.id=b.id and a.id=b.id  left join c on b.id=c.id and b.id=c.id left join d on c.id=d.id and c.id=d.id ";
        execute(dataSet, sql);
        assertEquals("[[a, b, c, d]]", successCollections.toString());
    }

    @Test
    public void leftJoin1() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a left join b on a.id=b.id inner join c on b.id=c.id and b.id=c.id";
        execute(dataSet, sql);
        assertEquals("[[a, b, c]]", successCollections.toString());
    }


    @Test
    public void leftJoin2() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a left join b on a.id=b.id inner join c on b.id=c.id and a.id=c.id";
        execute(dataSet, sql);
        assertEquals("[[a, b, c], [a, c, b]]", successCollections.toString());
    }


    @Test
    public void leftJoin3() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a left join b on a.id=b.id left join c on a.id=c.id  and b.id=1";
        execute(dataSet, sql);
        assertEquals("[[a, b, c]]", successCollections.toString());
    }

    @Test
    public void leftJoin4() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a left join b on a.id=b.id and b.id=1 left join c on a.id=c.id  ";
        execute(dataSet, sql);
        assertEquals("[[a, b, c], [a, c, b]]", successCollections.toString());
    }

    @Test
    public void leftJoin5() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a left join b on a.id=b.id  left join c on a.id=c.id  where b.id=1 ";
        execute(dataSet, sql);
        assertEquals("[[a, b, c], [a, c, b]]", successCollections.toString());
    }

    @Test
    public void circle() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id and c.id=a.id";
        execute(dataSet, sql);
        assertEquals(0, wrongCollections.size());
    }

    @Test
    public void diamond() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id  inner join d on a.id=d.id and d.id=c.id ";
        execute(dataSet, sql);
        assertEquals(4 * 4, successCollections.size());
        assertEquals("[[a, b, c, d], [a, b, d, c], [a, d, b, c], [d, a, b, c], [d, a, c, b], [a, d, c, b], [c, d, a, b], [d, c, a, b], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, a, d, c], [b, a, c, d]]", successCollections.toString());

    }

    @Test
    public void diamondWithDiagonal() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id  inner join d on a.id=d.id and d.id=c.id and d.id=b.id";
        execute(dataSet, sql);
        assertEquals(4 * 4 + 2 + 2, successCollections.size());
        assertEquals("[[a, b, c, d], [a, b, d, c], [a, d, b, c], [d, a, b, c], [d, a, c, b], [a, d, c, b], [c, d, a, b], [d, c, a, b], [d, c, b, a], [c, d, b, a], [c, b, d, a], [c, b, a, d], [b, c, a, d], [b, c, d, a], [b, d, c, a], [d, b, c, a], [d, b, a, c], [b, d, a, c], [b, a, d, c], [b, a, c, d]]", successCollections.toString());

    }

    @Test
    public void diamondWithDiagonal2() throws Exception {
        Collection<List<String>> dataSet = Collections2.permutations(Lists.newArrayList("a", "b", "c", "d"));
        sql = "select * from a inner join b on a.id=b.id inner join c on b.id=c.id and c.id=a.id inner join d on a.id=d.id and d.id=c.id and d.id=b.id";
        execute(dataSet, sql);
        assertEquals(dataSet.size(), successCollections.size());
    }

    private void verifyHint(String sql, List<String> arr) throws Exception {

        final HintPlanInfo info = new HintPlanInfo();
        for (int i = 0; i < arr.size(); ) {
            final List<String> list = arr.subList(i, RANDOM.nextInt(i, arr.size()) + 1);
            info.getGroups().add(HintPlanNodeGroup.of(HintPlanNodeGroup.Type.AND, list));
            i += list.size();
        }
        if (info.size() != arr.size()) {
            throw new RuntimeException("code error");
        }
        LOGGER.info("hintInfo {}", info);
        final RouteResultset rrs = RouteService.getInstance().route(schemaConfig, ServerParse.SELECT, sql, shardingService);
        PlanNode node = getPlanNode(rrs);
        MyOptimizer.optimize(node, info);
    }

    protected void execute(Collection<List<String>> dataSet, String sql) throws Exception {
        for (List<String> data : dataSet) {
            try {
                verifyHint(sql, data);
                successCollections.add(data);
            } catch (MySQLOutPutException e) {
                wrongCollections.add(data);
                LOGGER.warn("illegal state " + data + ", e:" + e);
            }
        }

        LOGGER.info("success collections is" + successCollections);
    }


}
