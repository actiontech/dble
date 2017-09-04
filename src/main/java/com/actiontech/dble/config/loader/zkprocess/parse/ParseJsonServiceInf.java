package com.actiontech.dble.config.loader.zkprocess.parse;

/**
 * author:liujun
 * Created:2016/9/16
 */
public interface ParseJsonServiceInf<T> {

    /**
     * parseBeanToJson
     *
     * @param data
     * @return
     * @Created 2016/9/16
     */
    String parseBeanToJson(T t);

    /**
     * parseJsonToBean
     *
     * @param json
     * @return
     * @Created 2016/9/16
     */
    T parseJsonToBean(String json);

}
