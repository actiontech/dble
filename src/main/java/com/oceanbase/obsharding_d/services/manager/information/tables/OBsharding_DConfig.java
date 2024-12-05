/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.function.Function;
import com.oceanbase.obsharding_d.cluster.zkprocess.parse.JsonProcessBase;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.converter.ShardingConverter;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DConfig extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding_d_config";

    private static final String COLUMN_CONTENT = "content";

    public OBsharding_DConfig() {
        super(TABLE_NAME, 1);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_CONTENT, new ColumnMeta(COLUMN_CONTENT, "text", false));
        columnsType.put(COLUMN_CONTENT, Fields.FIELD_TYPE_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        JsonObject resultJson = new JsonObject();
        List<RawJson> jsonStrList = Lists.newArrayList(OBsharding_DServer.getInstance().getConfig().getDbConfig(), OBsharding_DServer.getInstance().getConfig().getShardingConfig(),
                OBsharding_DServer.getInstance().getConfig().getUserConfig(), OBsharding_DServer.getInstance().getConfig().getSequenceConfig());
        for (RawJson jsonRaw : jsonStrList) {
            if (jsonRaw == null) {
                continue;
            }
            JsonObject jsonObj = jsonRaw.getJsonObject();
            if (null != jsonObj && !jsonObj.isJsonNull()) {
                jsonObj.entrySet().forEach(elementEntry -> {
                    if (elementEntry.getKey().contains("sequence")) {
                        //nothing
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
        rowMap.put(COLUMN_CONTENT, JsonFactory.getJson().toJson(resultJson));
        return Lists.newArrayList(rowMap);
    }
}
