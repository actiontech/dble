/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

import java.lang.ref.SoftReference;

public class UsageDataBlock extends SoftReference<UsageData> {

    public UsageDataBlock(String key, String value) {
        super(new UsageData(key, value));
    }

}
