/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager;

import com.actiontech.dble.manager.handler.ShowServerLog;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created by szf on 2017/7/28.
 */
public class ShowServerLogTest {

    @Test
    public void testPase() {
        String[] testlist = {
                "log @@ file =    logname  limit  = rowLimit          key  =  'keyWord'  regex   =    regexStr",
                "log @@ file =    logname  limit  = rowLimit          key  =  keyWord  regex   =    regexStr",
                "log @@file=logname limit=rowLimit key='keyWord' regex=regexStr",
                "log @@file=lognamelimit=rowLimit key='keyWord' regex=regexStr",
                "log @@file=lognamelimit= rowLimit key='keyWord' regex=regexStr",
                "log @@xffd 123123",
                "log @@ file =    logname",
                "log @@ limit = rowLimit",
                "log @@  key  = 'keyWord'",
                "log @@  regex  = regexStr",
                "log @@ file =    logname limit = rowLimit    ",
                "log @@  limit = rowLimit     file =    logname",
                "log @@ file =    logname key = 'keyWord'    ",
                "log @@ key = 'keyWord'  file =    logname    ",
                "log @@ file =    logname  regex   =    [\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?  ",
                "log @@   regex   =    [\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?  file =    logname  ",
                "log @@ key = 'keyWord' limit = rowLimit    ",
                "log @@  limit = rowLimit   key = 'keyWord'  ",
                "log @@ key = 'keyWord'    regex   =    [\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?  ",
                "log @@  regex   =    [\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])? key = 'keyWord'   ",
                "log @@ key=234",
                "log @@ file = sdfsd asdfas",
                "log @@ file = mysql.log sdfasdffffff",
                "log @@ key = 'sdfsdf' asdfasdf",
                "log @@ regex = sdfasdf fgdfgfdfgf",
                " log @@ key = '123123' file = 123123 asdfasdf",
                " log @@ key = '123123' file = 123123 ",
                " log @@ key = 123123 asdfasdf file = 123123 "

        };
        String[] resultlist = {
                "logname||rowLimit||keyWord||regexStr",
                "logname||rowLimit||keyWord||regexStr",
                "logname||rowLimit||keyWord||regexStr",
                "lognamelimit=rowLimit||null||keyWord||regexStr",
                null,
                null,
                "logname||null||null||null",
                "null||rowLimit||null||null",
                "null||null||keyWord||null",
                "null||null||null||regexStr",
                "logname||rowLimit||null||null",
                "logname||rowLimit||null||null",
                "logname||null||keyWord||null",
                "logname||null||keyWord||null",
                "logname||null||null||[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?",
                "logname||null||null||[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?",
                "null||rowLimit||keyWord||null",
                "null||rowLimit||keyWord||null",
                "null||null||keyWord||[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?",
                "null||null||keyWord||[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?",
                "null||null||234||null",
                null,
                null,
                null,
                null,
                null,
                "123123||null||123123||null",
                null
        };

        for (int i = 0; i < testlist.length; i++) {
            System.out.println("" + i);
            Assert.assertTrue(mapEquals(ShowServerLog.getCondPair(testlist[i]), resultlist[i]));
        }


    }

    public boolean mapEquals(Map<String, String> m1, String value) {
        if (m1 == null && value == null) {
            return true;
        }
        String[] values = value.split("\\|\\|");
        if (!"null".equals(values[0])) {
            if (!m1.get("file").equals(values[0])) {
                return false;
            }
        }
        if (!"null".equals(values[1])) {
            if (!m1.get("limit").equals(values[1])) {
                return false;
            }
        }
        if (!"null".equals(values[2])) {
            if (!m1.get("key").equals(values[2])) {
                return false;
            }
        }
        if (!"null".equals(values[3])) {
            if (!m1.get("regex").equals(values[3])) {
                return false;
            }
        }
        return true;
    }
}
