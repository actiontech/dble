/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SetIgnoreUtil
 * <p>
 * PHP user use multi SET
 *
 * @author zhuam
 */
public final class SetIgnoreUtil {
    private SetIgnoreUtil() {
    }

    private static List<Pattern> ptrnIgnoreList = new ArrayList<>();

    static {

        //TODO: ignore SET
        String[] ignores = new String[]{
                "(?i)set (sql_mode)",
                "(?i)set (interactive_timeout|wait_timeout|net_read_timeout|net_write_timeout|lock_wait_timeout|slave_net_timeout)",
                "(?i)set (connect_timeout|delayed_insert_timeout|innodb_lock_wait_timeout|innodb_rollback_on_timeout)",
                "(?i)set (profiling|profiling_history_size)",
                "(?i)set (sql_safe_updates)",
        };

        for (String ignore : ignores) {
            ptrnIgnoreList.add(Pattern.compile(ignore));
        }
    }

    public static boolean isIgnoreStmt(String stmt) {
        boolean ignore = false;
        Matcher matcherIgnore;
        for (Pattern ptrnIgnore : ptrnIgnoreList) {
            matcherIgnore = ptrnIgnore.matcher(stmt);
            if (matcherIgnore.find()) {
                ignore = true;
                break;
            }
        }
        return ignore;
    }

}
