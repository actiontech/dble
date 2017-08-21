package io.mycat.manager;

import io.mycat.route.parser.ManagerParseHeartbeat;
import io.mycat.route.parser.util.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by szf on 2017/8/8.
 */
public class ShowHeartBeatDetailTest {

    @Test
    public void parseTest() {
        String[] testlist = {"show @@heartbeat.detail where name = hostM1",
                "show @@heartbeat.detail where name = hostM1",
                "show        @@heartbeat.detail         where           name          =       hostM1       ",
                "show        @@heartbeat.detail         where     name     =       host  M1       ",
                "show @@heartbeat.details where name = hostM1",
                "show @@heartbeat.detail wherename = hostM1",
                "show @@heartbeat.detail where name ：hostM1",
                "show @@heartbeat.detail where name = hossdfasdf  tM1",
                "show @@heartbeat.detailwhere name = hostM1",
                "show @@heartbeat.detail where name = ",
                "show @@heartbeat.detail where name",};
        String[] resultlist = {"name,hostM1",
                "name,hostM1",
                "name,hostM1",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        };
        for (int i = 0; i < testlist.length; i++) {
            System.out.println(i);
            Assert.assertTrue(mapEquals(ManagerParseHeartbeat.getPair(testlist[i]), resultlist[i]));
        }
    }

    public boolean mapEquals(Pair<String, String> pair, String s) {
        if (s == null) {
            if (pair == null || pair.getValue().length() == 0) {
                return true;
            }
            return false;
        }

        if (pair.getKey().equals(s.split(",")[0])
                && pair.getValue().equals(s.split(",")[1])) {
            return true;
        }
        return false;
    }
}
