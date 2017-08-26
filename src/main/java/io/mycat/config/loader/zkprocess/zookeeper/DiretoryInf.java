package io.mycat.config.loader.zkprocess.zookeeper;

import java.util.List;

/**
 * 目录接口信息
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public interface DiretoryInf {

    /**
     * 获取当前的目录信息
     *
     * @return
     */
    String getDiretoryInfo();

    /**
     * 添加目录或者数据节点
     *
     * @param branch
     */
    void add(DiretoryInf directory);

    /**
     * 添加数据节点信息
     * 方法描述
     *
     * @param data
     * @Created 2016/9/15
     */
    void add(DataInf data);

    /**
     * 获取子节点信息
     *
     * @return
     */
    List<Object> getSubordinateInfo();

    /**
     * 获取节点的名称
     *
     *
     */
    String getDataName();

}
