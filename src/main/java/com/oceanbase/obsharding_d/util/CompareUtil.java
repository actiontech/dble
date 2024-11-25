/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

    /**
     *
     * example
     * compare(null, "v1") < 0
     * compare("v1", "v1")  = 0
     * compare(null, null)   = 0
     * compare("v1", null) > 0
     * compare("1.0.0", "1.0.2") < 0
     * compare("1.0.2", "1.0.2a") < 0
     * compare("1.13.0", "1.12.1c") > 0
     * compare("V0.0.20170102", "V0.0.20170101") > 0
     *
     * @param version1
     * @param version2
     * @param separatorChars
     * @return
     * @since https://www.cnblogs.com/shihaiming/p/6286575.html
     */
    public static int versionCompare(String version1, String version2, String separatorChars) {
        if (StringUtil.equals(version1, version2)) {
            return 0;
        }
        if (version1 == null && version2 == null) {
            return 0;
        } else if (version1 == null) {
            return -1;
        } else if (version2 == null) {
            return 1;
        }

        final List<String> v1s = Lists.newArrayList(StringUtils.split(version1, separatorChars));
        final List<String> v2s = Lists.newArrayList(StringUtils.split(version2, separatorChars));

        int diff = 0;
        int minLength = Math.min(v1s.size(), v2s.size());
        String v1;
        String v2;
        for (int i = 0; i < minLength; i++) {
            v1 = v1s.get(i);
            v2 = v2s.get(i);
            diff = v1.length() - v2.length();
            if (0 == diff) {
                diff = v1.compareTo(v2);
            }
            if (diff != 0) {
                break;
            }
        }

        return (diff != 0) ? diff : v1s.size() - v2s.size();
    }


}
