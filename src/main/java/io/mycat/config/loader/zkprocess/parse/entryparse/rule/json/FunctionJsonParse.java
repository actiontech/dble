package io.mycat.config.loader.zkprocess.parse.entryparse.rule.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * FunctionJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class FunctionJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<Function>> {

    @Override
    public String parseBeanToJson(List<Function> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<Function> parseJsonToBean(String json) {

        Type parseType = new TypeToken<List<Function>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
