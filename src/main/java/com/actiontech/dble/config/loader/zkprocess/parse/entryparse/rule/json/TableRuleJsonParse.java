/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.rule.json;

import com.actiontech.dble.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * TableRuleJsonParse
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/17
 */
public class TableRuleJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<TableRule>> {

    @Override
    public String parseBeanToJson(List<TableRule> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<TableRule> parseJsonToBean(String json) {

        Type parseType = new TypeToken<List<TableRule>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
