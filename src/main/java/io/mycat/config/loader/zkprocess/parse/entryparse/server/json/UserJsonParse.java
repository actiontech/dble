package io.mycat.config.loader.zkprocess.parse.entryparse.server.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.server.User;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * UserJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class UserJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<User>> {

    @Override
    public String parseBeanToJson(List<User> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<User> parseJsonToBean(String json) {
        Type parseType = new TypeToken<List<User>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
