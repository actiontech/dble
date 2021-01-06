package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.*;

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
        List<String> jsonStrList = Lists.newArrayList(DbleServer.getInstance().getConfig().getDbConfig(), DbleServer.getInstance().getConfig().getShardingConfig(),
                DbleServer.getInstance().getConfig().getUserConfig(), DbleServer.getInstance().getConfig().getSequenceConfig());
        for (String jsonStr : jsonStrList) {
            if (StringUtil.isBlank(jsonStr)) {
                continue;
            }
            JsonObject jsonObj = jsonParser.parse(jsonStr).getAsJsonObject();
            if (null != jsonObj && !jsonObj.isJsonNull()) {
                jsonObj.entrySet().forEach(elementEntry -> {
                    if (elementEntry.getKey().contains("sequence")) {
                        resultJson.add(elementEntry.getKey(), jsonParser.parse(elementEntry.getValue().getAsString()).getAsJsonObject());
                    } else {
                        resultJson.add(elementEntry.getKey(), elementEntry.getValue());
                    }
                });
            }
        }
        LinkedHashMap<String, String> rowMap = Maps.newLinkedHashMap();
        rowMap.put(COLUMN_CONTENT, resultJson.toString());
        return Lists.newArrayList(rowMap);
    }
}
