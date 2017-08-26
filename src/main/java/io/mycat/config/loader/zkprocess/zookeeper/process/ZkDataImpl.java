package io.mycat.config.loader.zkprocess.zookeeper.process;

import io.mycat.config.loader.zkprocess.zookeeper.DataInf;

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
public class ZkDataImpl implements DataInf {

    /**
     * 名称信息
     *
     *
     */
    private String name;

    /**
     * 当前值信息
     *
     *
     */
    private String value;

    public ZkDataImpl(String name, String value) {
        super();
        this.name = name;
        this.value = value;
    }

    @Override
    public String getDataInfo() {
        return this.name + ":" + this.value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String getDataValue() {
        return this.value;
    }

}
