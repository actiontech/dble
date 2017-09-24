/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.sort;


import com.actiontech.dble.memory.unsafe.utils.JavaUtils;

/**
 * Created by zagnix on 2016/6/6.
 */
public class HashPartitioner {
    private int index = 0;

    public HashPartitioner(int i) {
        this.index = i;
    }

    public int getPartition(String key) {
        return JavaUtils.nonNegativeMod(key.hashCode(), index);
    }

}
