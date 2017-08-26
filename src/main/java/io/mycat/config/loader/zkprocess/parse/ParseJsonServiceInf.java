package io.mycat.config.loader.zkprocess.parse;

/**
 * json转化服务
 *
 *
 * author:liujun
 * Created:2016/9/16
 *
 *
 *
 *
 */
public interface ParseJsonServiceInf<T> {

    /**
     * 将对象T转换为json字符串
     * 方法描述
     *
     * @param data
     * @return
     * @Created 2016/9/16
     */
    String parseBeanToJson(T t);

    /**
     * 将json字符串转换为javabean对象
     * 方法描述
     *
     * @param json
     * @return
     * @Created 2016/9/16
     */
    T parseJsonToBean(String json);

}
