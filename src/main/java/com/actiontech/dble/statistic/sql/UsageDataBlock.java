package com.actiontech.dble.statistic.sql;

import java.lang.ref.SoftReference;

public class UsageDataBlock extends SoftReference<UsageData> {

    public UsageDataBlock(String key, String value) {
        super(new UsageData(key, value));
    }

}
