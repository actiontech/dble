/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompareUtil {
    private CompareUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CompareUtil.class);

    public static int compareInt(int l, int r) {

        return Integer.compare(l, r);

    }

    public static int compareDouble(double l, double r) {

        return Double.compare(l, r);

    }

    public static int compareFloat(float l, float r) {

        return Float.compare(l, r);

    }

    public static int compareLong(long l, long r) {
        return Long.compare(l, r);

    }

    public static int compareString(String l, String r) {
        if (l == null) {
            return -1;
        } else if (r == null) {
            return 1;
        }
        return l.compareTo(r);
    }

    public static int compareChar(char l, char r) {

        return Character.compare(l, r);

    }

    public static int compareUtilDate(Object left, Object right) {

        java.util.Date l = (java.util.Date) left;
        java.util.Date r = (java.util.Date) right;

        return l.compareTo(r);

    }

    public static int compareSqlDate(Object left, Object right) {

        java.sql.Date l = (java.sql.Date) left;
        java.sql.Date r = (java.sql.Date) right;

        return l.compareTo(r);

    }



    private static int getCharCode(String s) {
        if (s == null || s.length() == 0) {
            return -1;
        }
        byte[] b = s.getBytes();
        int value = 0;
        //get first char
        for (int i = 0; i < b.length && i <= 2; i++) {
            value = value * 100 + b[i];
        }
        if (value < 0) {
            value += 100000;
        }

        return value;
    }

    private static int chineseCompareTo(String s1, String s2) {
        int len1 = s1.length();
        int len2 = s2.length();

        int n = Math.min(len1, len2);

        for (int i = 0; i < n; i++) {
            int s1Code = getCharCode(s1.charAt(i) + "");
            int s2Code = getCharCode(s2.charAt(i) + "");
            if (s1Code != s2Code) {
                return s1Code - s2Code;
            }
        }
        return len1 - len2;
    }
}
