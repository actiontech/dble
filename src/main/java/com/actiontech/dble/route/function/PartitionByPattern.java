/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * auto partition by Long
 *
 * @author hexiaobin
 */
public class PartitionByPattern extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByPattern.class);
    private static final int PARTITION_LENGTH = 1024;
    private int patternValue = PARTITION_LENGTH; // mod value
    private String mapFile = null;
    private String ruleFile = null;
    private LongRange[] longRanges;
    private Integer[] allNode;
    private int defaultNode = -1; // default node for unexpected value
    private static final Pattern PATTERN = Pattern.compile("[0-9]*");
    private int hashCode = 1;

    @Override
    public void init() {
        initialize();
        initHashCode();
    }

    @Override
    public void selfCheck() {
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }
    public String getMapFile() {
        return mapFile;
    }


    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    public void setPatternValue(int patternValue) {
        this.patternValue = patternValue;
        propertiesMap.put("patternValue", String.valueOf(patternValue));
    }

    public void setDefaultNode(int defaultNode) {
        if (defaultNode >= 0 || defaultNode == -1) {
            this.defaultNode = defaultNode;
        } else {
            LOGGER.warn("pattern range algorithm default node less than 0 and is not -1, use -1 replaced.");
        }
        propertiesMap.put("defaultNode", String.valueOf(defaultNode));
    }

    private Integer findNode(long hash) {
        for (LongRange longRang : this.longRanges) {
            if (hash <= longRang.getValueEnd() && hash >= longRang.getValueStart()) {
                return longRang.getNodeIndex();
            }
        }
        return null;
    }

    @Override
    public Integer calculate(String columnValue) {
        if (columnValue == null || columnValue.equalsIgnoreCase("NULL") || !isNumeric(columnValue)) {
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
        for (LongRange longRang : this.longRanges) {
            if (longRang.getValueEnd() < begin) {
                continue;
            }
            if (longRang.getValueStart() > end) {
                break;
            }
            ids.add(longRang.getNodeIndex());
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
        return this.longRanges.length;
    }

    private static boolean isNumeric(String str) {
        return PATTERN.matcher(str).matches();
    }

    private void initializeAux(LinkedList<LongRange> ll, LongRange lr) {
        if (ll.size() == 0) {
            ll.add(lr);
        } else {
            LongRange tmp;
            for (int i = ll.size() - 1; i > -1; i--) {
                tmp = ll.get(i);
                if (tmp.getValueStart() < lr.getValueStart()) {
                    ll.add(i + 1, lr);
                    return;
                }
            }
            ll.add(0, lr);
        }
    }

    private void initialize() {
        StringBuilder sb = new StringBuilder("{");
        BufferedReader in = null;
        try {
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));
            String fileName = mapFile != null ? mapFile : ruleFile;
            if (mapFile != null && ruleFile != null) {
                throw new RuntimeException("Configuration duplication in " + this.getClass().getName() + " ruleFile & mapFile both exist");
            } else if (mapFile == null && ruleFile == null) {
                throw new RuntimeException("One of the ruleFile and mapFile need config in " + this.getClass().getName());
            }

            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(fileName);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            LinkedList<LongRange> longRangeList = new LinkedList<>();
            HashSet<Integer> ids = new HashSet<>();
            int iRow = 0;
            for (String line; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }

                int ind = line.indexOf('=');
                if (ind < 0) {
                    LOGGER.info(" warn: bad line int " + mapFile + " :" + line);
                    continue;
                }

                String key = line.substring(0, ind).trim();
                String[] pairs = key.split("-");
                long longStart = Long.parseLong(pairs[0].trim());
                long longEnd = Long.parseLong(pairs[1].trim());
                String value = line.substring(ind + 1).trim();
                int nodeId = Integer.parseInt(value);

                ids.add(nodeId);
                initializeAux(longRangeList, new LongRange(nodeId, longStart, longEnd));
                if (iRow > 0) {
                    sb.append(",");
                }
                iRow++;
                sb.append("\"");
                sb.append(key);
                sb.append("\":");
                sb.append("\"");
                sb.append(value);
                sb.append("\"");
            }

            allNode = ids.toArray(new Integer[ids.size()]);
            longRanges = longRangeList.toArray(new LongRange[longRangeList.size()]);
            sb.append("}");
            propertiesMap.put("mapFile", sb.toString());
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                //ignore error
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByPattern other = (PartitionByPattern) o;
        if (other.defaultNode != defaultNode) {
            return false;
        }
        if (other.patternValue != patternValue) {
            return false;
        }
        for (int i = 0; i < longRanges.length; i++) {
            if (!other.longRanges[i].equals(longRanges[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        hashCode *= patternValue;
        if (defaultNode != 0) {
            hashCode *= defaultNode;
        }
        for (LongRange longRange : longRanges) {
            hashCode *= longRange.hashCode();
        }
    }
}
