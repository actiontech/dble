package io.mycat.config.loader.zkprocess.parse;

/**
 * xml转化服务
 *
 *
 * author:liujun
 * Created:2016/9/16
 *
 *
 *
 *
 */
public interface ParseXmlServiceInf<T> {

    /**
     * 将对象T写入xml文件
     * 方法描述
     *
     * @param data
     * @return
     * @Created 2016/9/16
     */
    void parseToXmlWrite(T data, String outputPath, String dataName);

    /**
     * 将指定的xml转换为javabean对象
     * 方法描述
     *
     * @param path xml文件路径信息
     * @return
     * @Created 2016/9/16
     */
    T parseXmlToBean(String path);

}
