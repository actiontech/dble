package io.mycat.config.loader.zkprocess.parse.entryparse.server.json;

import io.mycat.config.loader.zkprocess.entity.server.System;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * SystemJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class SystemJsonParse extends JsonProcessBase implements ParseJsonServiceInf<System> {

    @Override
    public String parseBeanToJson(System t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public System parseJsonToBean(String json) {

        return this.toBeanformJson(json, System.class);
    }

}
