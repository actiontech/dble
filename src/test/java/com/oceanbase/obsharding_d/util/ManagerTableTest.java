package com.oceanbase.obsharding_d.util;

import com.oceanbase.obsharding_d.services.manager.information.ManagerTableUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ManagerTableTest {

    @Test
    public void check() {
        List<String> realList = ManagerTableUtil.getTables("schema", "select * from test");
        Assert.assertEquals("schema.test", realList.get(0));

        realList = ManagerTableUtil.getTables("schema", "select * from testdb.test");
        Assert.assertEquals("testdb.test", realList.get(0));

        realList = ManagerTableUtil.getTables("schema", "select * from `testdb`.`test`");
        Assert.assertEquals("testdb.test", realList.get(0));

        realList = ManagerTableUtil.getTables("schema", "select * from `testdb`.test inner join test1");
        Assert.assertEquals("testdb.test", realList.get(0));
        Assert.assertEquals("schema.test1", realList.get(1));


        realList = ManagerTableUtil.getTables("schema", "/*!OBsharding-D:shardingNode=dn1*/ select n.id,s.name from sharding_2_t1 n join sharding_4_t1 s on n.id=s.id ");
        Assert.assertEquals("schema.sharding_2_t1", realList.get(0));
        Assert.assertEquals("schema.sharding_4_t1", realList.get(1));

        realList = ManagerTableUtil.getTables("schema", "/*#OBsharding-D:sharding=DN1*/select n.id,s.name from sharding_2_t1 n join sharding_4_t1 s on n.id=s.id ");
        Assert.assertEquals("schema.sharding_2_t1", realList.get(0));
        Assert.assertEquals("schema.sharding_4_t1", realList.get(1));

        realList = ManagerTableUtil.getTables("schema", "/*#OBsharding-D:sql = SELECT id FROM user*/select n.id,s.name from sharding_2_t1 n join sharding_4_t1 s on n.id=s.id ");
        Assert.assertEquals("schema.sharding_2_t1", realList.get(0));
        Assert.assertEquals("schema.sharding_4_t1", realList.get(1));

        realList = ManagerTableUtil.getTables("schema", "select n.id,s.name from sharding_2_t1 n join sharding_2_t1 s on n.id=s.id ");
        Assert.assertEquals(2, realList.size());
    }
}
