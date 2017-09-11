/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
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

    @Override
    public void init() {
        try {
            partitionTime = Integer.parseInt(sPartionDay) * ONE_DAY;

            beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

            if (sEndDate != null && !sEndDate.equals("")) {
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
    }

    @Override
    public Integer calculate(String columnValue) {
        try {
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
            LOGGER.error("error", e);
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
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }
}
