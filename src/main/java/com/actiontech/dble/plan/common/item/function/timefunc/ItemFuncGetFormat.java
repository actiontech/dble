/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;

import java.util.List;

public class ItemFuncGetFormat extends ItemStrFunc {
    public ItemFuncGetFormat(List<Item> args) {
        super(args);
    }

    @Override
    public String funcName() {
        return "GET_FORMAT";
    }

    @Override
    public String valStr() {
        String agr0 = args.get(0).valStr().toUpperCase();
        String agr1 = args.get(1).valStr().toUpperCase();
        if ("DATE".equals(agr0)) {
            if ("USA".equals(agr1)) {
                return "%m.%d.%Y";
            } else if ("JIS".equals(agr1) || "ISO".equals(agr1)) {
                return "%Y-%m-%d";
            } else if ("EUR".equals(agr1)) {
                return "%d.%m.%Y";
            } else if ("INTERNAL".equals(agr1)) {
                return "%Y%m%d";
            }
        } else if ("DATETIME".equals(agr0)) {
            if ("USA".equals(agr1) || "EUR".equals(agr1)) {
                return "%Y-%m-%d %H.%i.%s";
            } else if ("JIS".equals(agr1) || "ISO".equals(agr1)) {
                return "%Y-%m-%d %H:%i:%s";
            } else if ("INTERNAL".equals(agr1)) {
                return "%Y%m%d%H%i%s";
            }
        } else if ("TIME".equals(agr0)) {
            if ("USA".equals(agr1)) {
                return "%h:%i:%s %p";
            } else if ("JIS".equals(agr1) || "ISO".equals(agr1)) {
                return "%H:%i:%s";
            } else if ("EUR".equals(agr1)) {
                return "%H.%i.%s";
            } else if ("INTERNAL".equals(agr1)) {
                return "%H%i%s";
            }
        }
        return null;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncGetFormat(realArgs);
    }
}
