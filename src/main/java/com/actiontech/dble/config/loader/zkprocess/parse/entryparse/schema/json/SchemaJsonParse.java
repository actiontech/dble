/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json;

import com.actiontech.dble.config.loader.zkprocess.entity.schema.schema.Schema;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * author:liujun
 * Created:2016/9/17
 */
public class SchemaJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<Schema>> {

    @Override
    public String parseBeanToJson(List<Schema> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<Schema> parseJsonToBean(String json) {
        Type parseType = new TypeToken<List<Schema>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
