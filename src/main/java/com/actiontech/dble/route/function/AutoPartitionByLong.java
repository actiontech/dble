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
import java.util.LinkedList;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 *
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private static final long serialVersionUID = 5752372920655270639L;
    private String mapFile;
    private LongRange[] longRongs;

    private int defaultNode = -1;

    @Override
    public void init() {

        initialize();
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    @Override
    public Integer calculate(String columnValue) {
        //columnValue = NumberParseUtil.eliminateQoute(columnValue);
        try {
            long value = Long.parseLong(columnValue);
            Integer rst = null;
            for (LongRange longRang : this.longRongs) {
                if (value <= longRang.valueEnd && value >= longRang.valueStart) {
                    return longRang.nodeIndx;
                }
            }
            // use default node for other value
            if (rst == null && defaultNode >= 0) {
                return defaultNode;
            }
            return rst;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    /**
     * @param columnValue
     * @return
     */
    public boolean isUseDefaultNode(String columnValue) {
        try {
            long value = Long.parseLong(columnValue);
            Integer rst = null;
            for (LongRange longRang : this.longRongs) {
                if (value <= longRang.valueEnd && value >= longRang.valueStart) {
                    return false;
                }
            }
            if (rst == null && defaultNode >= 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
        return false;
    }


    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        Integer begin = 0, end = 0;
        if (isUseDefaultNode(beginValue) || isUseDefaultNode(endValue)) {
            begin = 0;
            end = longRongs.length - 1;
        } else {
            begin = calculate(beginValue);
            end = calculate(endValue);
        }


        if (begin == null || end == null) {
            return new Integer[0];
        }
        if (end >= begin) {
            int len = end - begin + 1;
            Integer[] re = new Integer[len];

            for (int i = 0; i < len; i++) {
                re[i] = begin + i;
            }
            return re;
        } else {
            return new Integer[0];
        }
    }

    @Override
    public int getPartitionNum() {
        int nPartition = longRongs.length;
        return nPartition;
    }

    private void initialize() {
        BufferedReader in = null;
        try {
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));
            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            LinkedList<LongRange> longRangeList = new LinkedList<>();

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
                long longStart = NumberParseUtil.parseLong(pairs[0].trim());
                long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
                int nodeId = Integer.parseInt(line.substring(ind + 1).trim());
                longRangeList.add(new LongRange(nodeId, longStart, longEnd));

            }
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

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
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
