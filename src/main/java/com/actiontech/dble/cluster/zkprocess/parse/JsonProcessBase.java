/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.parse;

import com.google.gson.Gson;

import java.lang.reflect.Type;

/**
 * JsonProcessBase
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
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
     * @param classinfo
     * @return
     * @Created 2016/9/17
     */
    public <T> T toBeanformJson(String json, Class<T> classinfo) {
        T result = this.gson.fromJson(json, classinfo);

        return result;
    }
}
