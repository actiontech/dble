/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.ResourceUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * auto partition by Long
 *
 * @author hexiaobin
 */
public class PartitionByPattern extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final int PARTITION_LENGTH = 1024;
    private int patternValue = PARTITION_LENGTH; // mod value
    private String mapFile;
    private LongRange[] longRongs;
    private Integer[] allNode;
    private int defaultNode = -1; // default node for unexpected value
    private static final Pattern PATTERN = Pattern.compile("[0-9]*");

    @Override
    public void init() {
        initialize();
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    public void setPatternValue(int patternValue) {
        this.patternValue = patternValue;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }

    private Integer findNode(long hash) {
        for (LongRange longRang : this.longRongs) {
            if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
                return longRang.nodeIndx;
            }
        }
        return null;
    }

    @Override
    public Integer calculate(String columnValue) {
        if (!isNumeric(columnValue)) {
            return defaultNode < 0 ? null : defaultNode;
        }

        long value = Long.parseLong(columnValue);
        long hash = value % patternValue;
        return findNode(hash);
    }

    /* x2 - x1 < m
 *     n1 < n2 ---> n1 - n2          type1
 *     n1 > n2 ---> 0 - n2 && n1 - L      type2
 * x2 - x1 >= m
 *     L                 type3
 */
    private void calcAux(HashSet<Integer> ids, long begin, long end) {
        for (LongRange longRang : this.longRongs) {
            if (longRang.valueEnd < begin) {
                continue;
            }
            if (longRang.valueStart > end) {
                break;
            }
            ids.add(longRang.nodeIndx);
        }
    }

    private Integer[] calcType1(long begin, long end) {
        HashSet<Integer> ids = new HashSet<>();

        calcAux(ids, begin, end);

        return ids.toArray(new Integer[ids.size()]);
    }

    private Integer[] calcType2(long begin, long end) {
        HashSet<Integer> ids = new HashSet<>();

        calcAux(ids, begin, patternValue);
        calcAux(ids, 0, end);

        return ids.toArray(new Integer[ids.size()]);
    }

    private Integer[] calcType3() {
        return allNode;
    }

    /* NODE:
 * if the order of range and the order of nodeid don't match, we give all nodeid.
 * so when writing configure, be cautious
 */
    public Integer[] calculateRange(String beginValue, String endValue) {
        if (!isNumeric(beginValue) || !isNumeric(endValue)) {
            return calcType3();
        }
        long bv = Long.parseLong(beginValue);
        long ev = Long.parseLong(endValue);
        long hbv = bv % patternValue;
        long hev = ev % patternValue;

        if (findNode(hbv) == null || findNode(hev) == null) {
            return calcType3();
        }

        if (ev >= bv) {
            if (ev - bv >= patternValue) {
                return calcType3();
            }

            if (hbv < hev) {
                return calcType1(hbv, hev);
            } else {
                return calcType2(hbv, hev);
            }
        } else {
            return new Integer[0];
        }
    }

    @Override
    public int getPartitionNum() {
        int nPartition = this.longRongs.length;
        return nPartition;
    }

    public static boolean isNumeric(String str) {
        return PATTERN.matcher(str).matches();
    }

    private void initializeAux(LinkedList<LongRange> ll, LongRange lr) {
        if (ll.size() == 0) {
            ll.add(lr);
        } else {
            LongRange tmp;
            for (int i = ll.size() - 1; i > -1; i--) {
                tmp = ll.get(i);
                if (tmp.valueStart < lr.valueStart) {
                    ll.add(i + 1, lr);
                    return;
                }
            }
            ll.add(0, lr);
        }
    }

    private void initialize() {
        BufferedReader in = null;
        try {
            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            LinkedList<LongRange> longRangeList = new LinkedList<>();
            HashSet<Integer> ids = new HashSet<>();

            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }

                int ind = line.indexOf('=');
                if (ind < 0) {
                    System.out.println(" warn: bad line int " + mapFile + " :" + line);
                    continue;
                }

                String[] pairs = line.substring(0, ind).trim().split("-");
                long longStart = Long.parseLong(pairs[0].trim());
                long longEnd = Long.parseLong(pairs[1].trim());
                int nodeId = Integer.parseInt(line.substring(ind + 1).trim());

                ids.add(nodeId);
                initializeAux(longRangeList, new LongRange(nodeId, longStart, longEnd));
            }

            allNode = ids.toArray(new Integer[ids.size()]);
            longRongs = longRangeList.toArray(new LongRange[longRangeList.size()]);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                in.close();
            } catch (Exception e2) {
                //ignore error
            }
        }
    }

    static class LongRange implements Serializable {
        public final int nodeIndx;
        public final long valueStart;
        public final long valueEnd;

        LongRange(int nodeIndx, long valueStart, long valueEnd) {
            super();
            this.nodeIndx = nodeIndx;
            this.valueStart = valueStart;
            this.valueEnd = valueEnd;
        }
    }
}
