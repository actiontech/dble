/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

public final class NumberParseUtil {
    private NumberParseUtil() {
    }

    /**
     * remove the ' at the beginning and the end
     *
     * @param number
     * @return
     */
    public static String eliminateQoute(String number) {
        number = number.trim();
        if (number.contains("\"")) {
            if (number.charAt(0) == '\"') {
                number = number.substring(1);
                if (number.charAt(number.length() - 1) == '\"') {
                    number = number.substring(0, number.length() - 1);
                }
            }
        } else if (number.contains("\'")) {
            if (number.charAt(0) == '\'') {
                number = number.substring(1);
                if (number.charAt(number.length() - 1) == '\'') {
                    number = number.substring(0, number.length() - 1);
                }
            }
        }
        return number;
    }

    /**
     * can parse values like 200M ,200K,200M1(2000001)
     *
     * @param val
     * @return
     */
    public static long parseLong(String val) {
        val = val.toUpperCase();
        int indx = val.indexOf("M");

        int plus = 10000;
        if (indx < 0) {
            indx = val.indexOf("K");
            plus = 1000;
        }
        if (indx > 0) {
            String longVal = val.substring(0, indx);

            long theVale = Long.parseLong(longVal) * plus;
            String remain = val.substring(indx + 1);
            if (remain.length() > 0) {
                theVale += Integer.parseInt(remain);
            }
            return theVale;
        } else {
            return Long.parseLong(val);
        }

    }
}
