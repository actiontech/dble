package io.mycat.config.loader.zkprocess.zookeeper.process;

import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;

import java.util.ArrayList;
import java.util.List;

/**
 * zk的目录节点信息
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class ZkDirectoryImpl implements DiretoryInf {

    /**
     * 整个节点信息
     *
     *
     */
    private List<Object> subordinateInfoList = new ArrayList<>();

    /**
     * 节点的名称信息
     *
     *
     */
    private String name;

    /**
     * 当前节点的数据信息
     *
     *
     */
    private String value;

    public ZkDirectoryImpl(String name, String value) {
        this.name = name;
        this.value = value;
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
    public String getDiretoryInfo() {
        return name + ":" + value;
    }

    @Override
    public void add(DiretoryInf branch) {
        this.subordinateInfoList.add(branch);
    }

    @Override
    public void add(DataInf data) {
        this.subordinateInfoList.add(data);
    }

    @Override
    public List<Object> getSubordinateInfo() {
        return this.subordinateInfoList;
    }

    @Override
    public String getDataName() {
        return this.name;
    }

}
