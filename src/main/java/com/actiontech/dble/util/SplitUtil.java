/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author mycat
 */
public final class SplitUtil {
    private SplitUtil() {
    }

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * split<br>
     * eg:c1='$',c2='-' input:mysql_db$0-2<br>
     * output array:mysql_db[0],mysql_db[1],mysql_db[2]
     */
    public static String[] split2(String src, char c1, char c2) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<>();
        String[] p = split(src, c1, true);
        if (p.length > 1) {
            String[] scope = split(p[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            for (int x = min; x <= max; x++) {
                list.add(p[0] + '[' + x + ']');
            }
        } else {
            list.add(p[0]);
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src) {
        return split(src, null, -1);
    }

    public static String[] split(String src, char separatorChar) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    list.add(src.substring(start, i));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char separatorChar, boolean trim) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    if (trim) {
                        list.add(src.substring(start, i).trim());
                    } else {
                        list.add(src.substring(start, i));
                    }
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            if (trim) {
                list.add(src.substring(start, i).trim());
            } else {
                list.add(src.substring(start, i));
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String str, String separatorChars) {
        return split(str, separatorChars, -1);
    }

    public static String[] split(String src, String separatorChars, int max) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<>();
        int sizePlus1 = 1;
        int i = 0;
        int start = 0;
        boolean match = false;
        if (separatorChars == null) { // null means use whitespace as Separator
            while (i < length) {
                if (Character.isWhitespace(src.charAt(i))) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            char sep = separatorChars.charAt(0);
            while (i < length) {
                if (src.charAt(i) == sep) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else {
            while (i < length) {
                if (separatorChars.indexOf(src.charAt(i)) >= 0) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * parser String,eg: <br>
     * 1. c1='$',c2='-',c3='[',c4=']' input:mysql_db$0-2<br>
     * output:mysql_db[0],mysql_db[1],mysql_db[2]<br>
     * 2. c1='$',c2='-',c3='#',c4='0' input:mysql_db$0-2<br>
     * output:mysql_db#0,mysql_db#1,mysql_db#2<br>
     * 3. c1='$',c2='-',c3='0',c4='0' input:mysql_db$0-2<br>
     * output:mysql_db0,mysql_db1,mysql_db2<br>
     */
    public static String[] split(String src, char c1, char c2, char c3, char c4) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<>();
        if (src.indexOf(c1) == -1) {
            list.add(src.trim());
        } else {
            String[] s = split(src, c1, true);
            String[] scope = split(s[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            if (c3 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(s[0] + x);
                }
            } else if (c4 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(s[0] + c3 + x);
                }
            } else {
                for (int x = min; x <= max; x++) {
                    list.add(s[0] + c3 + x + c4);
                }
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char fi, char se, char th) {
        return split(src, fi, se, th, '0', '0');
    }

    public static String[] split(String src, char fi, char se, char th, char left, char right) {
        List<String> list = new LinkedList<>();
        String[] pools = split(src, fi, true);
        for (String pool : pools) {
            if (pool.indexOf(se) == -1) {
                list.add(pool);
                continue;
            }
            String[] s = split(pool, se, th, left, right);
            Collections.addAll(list, s);
        }
        return list.toArray(new String[list.size()]);
    }

}
