package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json;

import com.actiontech.dble.config.loader.zkprocess.entity.server.Alarm;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * Created by szf on 2018/3/5.
 */
public class AlarmJsonParse extends JsonProcessBase implements ParseJsonServiceInf<Alarm> {
    @Override
    public String parseBeanToJson(Alarm alarm) {
        return this.toJsonFromBean(alarm);
    }

    @Override
    public Alarm parseJsonToBean(String json) {
        return this.toBeanformJson(json, Alarm.class);
    }
}
