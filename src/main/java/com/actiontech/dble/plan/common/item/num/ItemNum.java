/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.num;

import com.actiontech.dble.plan.common.item.ItemBasicConstant;

public abstract class ItemNum extends ItemBasicConstant {

    public ItemNum() {
        // my_charset_numeric
        charsetIndex = 8;
    }

    public abstract ItemNum neg();
}
