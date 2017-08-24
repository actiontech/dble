package io.mycat.config.loader.zkprocess.parse;

import com.google.gson.Gson;

import java.lang.reflect.Type;

/**
 * json数据与实体类的类的信息
 * 源文件名：XmlProcessBase.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月15日
 * 修改作者：liujun
 * 修改日期：2016年9月15日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public class JsonProcessBase {

    /**
     * 进行消息转换的类的信息
     *
     * @字段说明 gson
     */
    private Gson gson = new Gson();

    /**
     * 进行json字符串化
     * 方法描述
     *
     * @param obj
     * @return
     * @创建日期 2016年9月17日
     */
    public String toJsonFromBean(Object obj) {
        if (null != obj) {
            return gson.toJson(obj);
        }

        return null;
    }

    /**
     * 将json字符串至类，根据指定的类型信息,一般用于集合的转换
     * 方法描述
     *
     * @param json
     * @param typeSchema
     * @return
     * @创建日期 2016年9月17日
     */
    public <T> T toBeanformJson(String json, Type typeSchema) {
        T result = this.gson.fromJson(json, typeSchema);

        return result;
    }

    /**
     * 将json字符串至类，根据指定的类型信息,用于转换单对象实体
     * 方法描述
     *
     * @param <T>
     * @param json
     * @param typeSchema
     * @return
     * @创建日期 2016年9月17日
     */
    public <T> T toBeanformJson(String json, Class<T> classinfo) {
        T result = this.gson.fromJson(json, classinfo);

        return result;
    }
}
