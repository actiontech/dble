package io.mycat.config.loader.zkprocess.zookeeper;

/**
 * 数据节点信息
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public interface DataInf {

    /**
     * 获取信息,以:分隔两个值
     *
     * @return
     */
    String getDataInfo();

    /**
     * 返回数据节点值信息
     * 方法描述
     *
     * @return
     * @Created 2016/9/17
     */
    String getDataValue();

}
