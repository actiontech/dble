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
import java.util.LinkedList;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 *
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 5752372920655270639L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoPartitionByLong.class);
    private String mapFile = null;
    private String ruleFile = null;
    private LongRange[] longRanges;
    private int defaultNode = -1;
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

    @Override
    public Integer calculate(String columnValue) {
        //columnValue = NumberParseUtil.eliminateQuote(columnValue);
        try {

            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                if (defaultNode >= 0) {
                    return defaultNode;
                }
                return null;
            }

            long value = Long.parseLong(columnValue);
            for (LongRange longRang : this.longRanges) {
                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                    return longRang.getNodeIndex();
                }
            }
            // use default node for other value
            if (defaultNode >= 0) {
                return defaultNode;
            }
            return null;
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
            for (LongRange longRang : this.longRanges) {
                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                    return false;
                }
            }
            if (defaultNode >= 0) {
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
            end = longRanges.length - 1;
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
        return longRanges.length;
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
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));
            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(fileName);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            LinkedList<LongRange> longRangeList = new LinkedList<>();
            int iRow = 0;
            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if ((line.length() == 0) || line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }
                int ind = line.indexOf('=');
                if (ind < 0) {
                    LOGGER.info(" warn: bad line int " + mapFile + " :" + line);
                    continue;
                }

                String key = line.substring(0, ind).trim();
                String[] pairs = key.split("-");
                long longStart = NumberParseUtil.parseLong(pairs[0].trim());
                long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
                String value = line.substring(ind + 1).trim();
                int nodeId = Integer.parseInt(value);
                longRangeList.add(new LongRange(nodeId, longStart, longEnd));
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

    public void setDefaultNode(int defaultNode) {
        if (defaultNode >= 0 || defaultNode == -1) {
            this.defaultNode = defaultNode;
        } else {
            LOGGER.warn("numberrange algorithm default node less than 0 and is not -1, use -1 replaced.");
        }
        propertiesMap.put("defaultNode", String.valueOf(defaultNode));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoPartitionByLong other = (AutoPartitionByLong) o;
        if (other.defaultNode != defaultNode) {
            return false;
        }
        if (other.longRanges.length != longRanges.length) {
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
        if (defaultNode != 0) {
            hashCode *= defaultNode;
        }
        for (LongRange longRange : longRanges) {
            hashCode *= longRange.hashCode();
        }
    }
}
