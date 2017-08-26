package io.mycat.config.loader.zkprocess.parse.entryparse.schema.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.schema.datahost.DataHost;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * DataHostJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class DataHostJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<DataHost>> {

    @Override
    public String parseBeanToJson(List<DataHost> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<DataHost> parseJsonToBean(String json) {
        Type parseType = new TypeToken<List<DataHost>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
