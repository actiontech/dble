/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.RawJson;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.function.Function;
import com.actiontech.dble.cluster.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.converter.ShardingConverter;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;

public class DbleConfig extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_config";

    private static final String COLUMN_CONTENT = "content";

    public DbleConfig() {
        super(TABLE_NAME, 1);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_CONTENT, new ColumnMeta(COLUMN_CONTENT, "text", false));
        columnsType.put(COLUMN_CONTENT, Fields.FIELD_TYPE_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        JsonParser jsonParser = new JsonParser();
        JsonObject resultJson = new JsonObject();
        List<RawJson> jsonStrList = Lists.newArrayList(DbleServer.getInstance().getConfig().getDbConfig(), DbleServer.getInstance().getConfig().getShardingConfig(),
                DbleServer.getInstance().getConfig().getUserConfig(), DbleServer.getInstance().getConfig().getSequenceConfig());
        for (RawJson jsonRaw : jsonStrList) {
            if (jsonRaw == null) {
                continue;
            }
            JsonObject jsonObj = jsonRaw.getJsonObject();
            if (null != jsonObj && !jsonObj.isJsonNull()) {
                jsonObj.entrySet().forEach(elementEntry -> {
                    if (elementEntry.getKey().contains("sequence")) {
                        elementEntry.setValue(jsonParser.parse(elementEntry.getValue().getAsString()).getAsJsonObject());
                    } else if (StringUtil.equals(elementEntry.getKey(), "function")) {
                        JsonProcessBase base = new JsonProcessBase();
                        Type parseType = new TypeToken<List<Function>>() {
                        }.getType();
                        List<Function> list = base.toBeanformJson(elementEntry.getValue().toString(), parseType);
                        ShardingConverter.removeFileContent(list);
                        elementEntry.setValue(new Gson().toJsonTree(list));
                    }
                    resultJson.add(elementEntry.getKey(), elementEntry.getValue());
                });
            }
        }
        LinkedHashMap<String, String> rowMap = Maps.newLinkedHashMap();
        rowMap.put(COLUMN_CONTENT, resultJson.toString());
        return Lists.newArrayList(rowMap);
    }
}
