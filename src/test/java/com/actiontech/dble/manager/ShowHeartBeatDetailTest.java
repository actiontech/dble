/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager;

import com.actiontech.dble.services.manager.response.ShowHeartbeatDetail;
import com.actiontech.dble.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Created by szf on 2017/8/8.
 */
public class ShowHeartBeatDetailTest {

    @Test
    public void parseTest() {
        Map<String, Boolean> testMap = new HashMap<>();
        testMap.put("show @@heartbeat.detail where name = hostM1", true);
        testMap.put("show @@heartbeat.detail where name =   'hostM1'", true);
        testMap.put("show @@heartbeat.detail where name =\"hostM1\"", true);
        testMap.put("show        @@heartbeat.detail         where           name          =       hostM1       ", true);
        testMap.put("show        @@heartbeat.detail         where           name=hostM1       ", true);
        testMap.put("show        @@heartbeat.detail         where           name          =hostM1       ", true);
        testMap.put("show        @@heartbeat.detail         where     name     =       host  M1       ", false);
        testMap.put("show @@heartbeat.details where name = hostM1", false);
        testMap.put("show @@heartbeat.detail wherename = hostM1", false);
        testMap.put("show @@heartbeat.detail where name :hostM1", false);
        testMap.put("show @@heartbeat.detail where name = hossdfasdf  tM1", false);
        testMap.put("show @@heartbeat.detailwhere name = hostM1", false);
        testMap.put("show @@heartbeat.detail where name = ", false);
        testMap.put("show @@heartbeat.detail where name", false);

        int i = 0;
        for (String k : testMap.keySet()) {
            System.out.println(i++);
            Assert.assertTrue(mapEquals(k, testMap.get(k)));
        }
    }

    public boolean mapEquals(String s, boolean expectedValue) {
        Matcher matcher = ShowHeartbeatDetail.HEARTBEAT_DETAIL_STMT.matcher(s);
        if (matcher.matches() &&
                (matcher.group(1) != null && StringUtil.removeAllApostrophe(matcher.group(1)).equalsIgnoreCase("name")) &&
                ((matcher.group(2) != null && StringUtil.removeAllApostrophe(matcher.group(2)).equalsIgnoreCase("hostM1")))) {
            return true == expectedValue;
        } else {
            return false == expectedValue;
        }
    }
}
