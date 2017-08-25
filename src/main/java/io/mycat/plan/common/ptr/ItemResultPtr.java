package io.mycat.plan.common.ptr;

import io.mycat.plan.common.item.Item.ItemResult;

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
