package io.mycat.config.loader.zkprocess.parse.entryparse.schema.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
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
