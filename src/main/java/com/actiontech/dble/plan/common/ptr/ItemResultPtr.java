/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

import com.actiontech.dble.plan.common.item.Item.ItemResult;

public class ItemResultPtr {
    private ItemResult itemResult;

    public ItemResultPtr(ItemResult itemResult) {
        this.itemResult = itemResult;
    }

    public ItemResult get() {
        return itemResult;
    }

    public void set(ItemResult result) {
        this.itemResult = result;
    }
}
