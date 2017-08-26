package io.mycat.config.loader.zkprocess.parse;

import java.lang.reflect.Type;

import com.google.gson.Gson;

/**
 * JsonProcessBase
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class JsonProcessBase {

    private Gson gson = new Gson();

    /**
     * toJsonFromBean
     *
     * @param obj
     * @return
     * @Created 2016/9/17
     */
    public String toJsonFromBean(Object obj) {
        if (null != obj) {
            return gson.toJson(obj);
        }

        return null;
    }

    /**
     * toBeanformJson
     *
     * @param json
     * @param typeSchema
     * @return
     * @Created 2016/9/17
     */
    public <T> T toBeanformJson(String json, Type typeSchema) {
        T result = this.gson.fromJson(json, typeSchema);

        return result;
    }

    /**
     * toBeanformJson
     *
     * @param <T>
     * @param json
     * @param typeSchema
     * @return
     * @Created 2016/9/17
     */
    public <T> T toBeanformJson(String json, Class<T> classinfo) {
        T result = this.gson.fromJson(json, classinfo);

        return result;
    }
}
