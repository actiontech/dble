/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

/**
 * @author mycat
 */
public final class PairUtil {
    private PairUtil() {
    }

    private static final int DEFAULT_INDEX = -1;

    /**
     * "2" -&gt; (0,2)<br/>
     * "1:2" -&gt; (1,2)<br/>
     * "1:" -&gt; (1,0)<br/>
     * "-1:" -&gt; (-1,0)<br/>
     * ":-1" -&gt; (0,-1)<br/>
     * ":" -&gt; (0,0)<br/>
     */
    public static Pair<Integer, Integer> sequenceSlicing(String slice) {
        int ind = slice.indexOf(':');
        if (ind < 0) {
            int i = Integer.parseInt(slice.trim());
            if (i >= 0) {
                return new Pair<>(0, i);
            } else {
                return new Pair<>(i, 0);
            }
        }
        String left = slice.substring(0, ind).trim();
        String right = slice.substring(1 + ind).trim();
        int start, end;
        if (left.length() <= 0) {
            start = 0;
        } else {
            start = Integer.parseInt(left);
        }
        if (right.length() <= 0) {
            end = 0;
        } else {
            end = Integer.parseInt(right);
        }
        return new Pair<>(start, end);
    }

    /**
     * <pre>
     * get the pair split by l and r
     * eg:src = "offer_group[4]", l='[', r=']' ,
     * return Piar<String,Integer>("offer", 4);
     * when src = "offer_group", l='[', r=']' ,
     * return Pair<String, Integer>("offer",-1);
     * </pre>
     */
    public static Pair<String, Integer> splitIndex(String src, char l, char r) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return new Pair<>("", DEFAULT_INDEX);
        }
        if (src.charAt(length - 1) != r) {
            return new Pair<>(src, DEFAULT_INDEX);
        }
        int offset = src.lastIndexOf(l);
        if (offset == -1) {
            return new Pair<>(src, DEFAULT_INDEX);
        }
        int index = DEFAULT_INDEX;
        try {
            index = Integer.parseInt(src.substring(offset + 1, length - 1));
        } catch (NumberFormatException e) {
            return new Pair<>(src, DEFAULT_INDEX);
        }
        return new Pair<>(src.substring(0, offset), index);
    }

}
