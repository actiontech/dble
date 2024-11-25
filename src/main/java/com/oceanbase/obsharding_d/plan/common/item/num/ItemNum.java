/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.num;

import com.oceanbase.obsharding_d.plan.common.item.ItemBasicConstant;

public abstract class ItemNum extends ItemBasicConstant {

    public ItemNum() {
        // my_charset_numeric
        charsetIndex = 8;
    }

    public abstract ItemNum neg();
}
