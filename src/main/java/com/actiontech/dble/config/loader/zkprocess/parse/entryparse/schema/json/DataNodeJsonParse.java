/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.json;

import com.actiontech.dble.config.loader.zkprocess.entity.schema.datanode.DataNode;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * DataNodeJsonParse
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/17
 */
public class DataNodeJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<DataNode>> {

    @Override
    public String parseBeanToJson(List<DataNode> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<DataNode> parseJsonToBean(String json) {
        Type parseType = new TypeToken<List<DataNode>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
