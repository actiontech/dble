/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * PartitionByDate
 *
 * @author lxy
 */
public class PartitionByDate extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 4966421543458534122L;

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDate.class);

    private String sBeginDate;
    private String sEndDate;
    private String sPartionDay;
    private String dateFormat;

    private long beginDate;
    private long partitionTime;
    private long endDate;
    private int nCount;
    private int defaultNode = -1;
    private transient ThreadLocal<SimpleDateFormat> formatter;
    private static final long ONE_DAY = 86400000;
    private int hashCode = -1;

    @Override
    public void init() {
        try {
            partitionTime = Integer.parseInt(sPartionDay) * ONE_DAY;

            beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

            if (!StringUtil.isEmpty(sEndDate)) {
                endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                nCount = (int) ((endDate - beginDate) / partitionTime) + 1;
            }
            formatter = new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(dateFormat);
                }
            };
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }

        initHashCode();
    }


    @Override
    public void selfCheck() {
        StringBuffer sb = new StringBuffer();

        if (sBeginDate == null || "".equals(sBeginDate)) {
            sb.append("sBeginDate can not be null\n");
        } else {
            try {
                new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();
            } catch (Exception e) {
                sb.append("pause beginDate error\n");
            }
        }

        if (dateFormat == null || "".equals(dateFormat)) {
            sb.append("dateFormat can not be null\n");
        } else {
            if (!StringUtil.isEmpty(sEndDate)) {
                try {
                    new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                } catch (Exception e) {
                    sb.append("pause endDate error\n");
                }
            }
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
            throw new RuntimeException(sb.toString());
        }
    }

    @Override
    public Integer calculate(String columnValue) {
        try {
            if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
                if (defaultNode >= 0) {
                    return defaultNode;
                }
                return null;
            }
            long targetTime = formatter.get().parse(columnValue).getTime();
            if (targetTime < beginDate) {
                return (defaultNode >= 0) ? defaultNode : null;
            }
            int targetPartition = (int) ((targetTime - beginDate) / partitionTime);

            if (targetTime > endDate && nCount != 0) {
                targetPartition = targetPartition % nCount;
            }
            return targetPartition;

        } catch (ParseException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
        try {
            Date begin = format.parse(beginValue);
            Date end = format.parse(endValue);
            Calendar cal = Calendar.getInstance();
            List<Integer> list = new ArrayList<>();
            while (begin.getTime() <= end.getTime()) {
                Integer nodeValue = this.calculate(format.format(begin));
                if (Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
                cal.setTime(begin);
                cal.add(Calendar.DATE, 1);
                begin = cal.getTime();
            }

            Integer[] nodeArray = new Integer[list.size()];
            for (int i = 0; i < list.size(); i++) {
                nodeArray[i] = list.get(i);
            }

            return nodeArray;
        } catch (ParseException e) {
            LOGGER.info("error", e);
            return new Integer[0];
        }
    }

    @Override
    public int getPartitionNum() {
        int count = this.nCount;
        return count > 0 ? count : -1;
    }

    public void setsBeginDate(String sBeginDate) {
        this.sBeginDate = sBeginDate;
        propertiesMap.put("sBeginDate", sBeginDate);
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
        propertiesMap.put("sPartionDay", sPartionDay);
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        propertiesMap.put("dateFormat", dateFormat);
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
        propertiesMap.put("sEndDate", sEndDate);
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
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
        PartitionByDate other = (PartitionByDate) o;

        return StringUtil.equals(other.sBeginDate, sBeginDate) &&
                StringUtil.equals(other.sPartionDay, sPartionDay) &&
                StringUtil.equals(other.dateFormat, dateFormat) &&
                StringUtil.equals(other.sEndDate, sEndDate) &&
                other.defaultNode == defaultNode;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        long tmpCode = beginDate;
        tmpCode *= partitionTime;
        if (defaultNode != 0) {
            tmpCode *= defaultNode;
        }
        if (!StringUtil.isEmpty(sEndDate)) {
            tmpCode *= endDate;
        }
        hashCode = (int) tmpCode;
    }
}
