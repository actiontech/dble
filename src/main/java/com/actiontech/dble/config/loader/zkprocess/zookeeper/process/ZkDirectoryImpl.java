/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DirectoryInf;

import java.util.ArrayList;
import java.util.List;

/**
 * ZkDirectoryImpl
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class ZkDirectoryImpl implements DirectoryInf {

    private List<Object> subordinateInfoList = new ArrayList<>();

    private String name;

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
    public String getDirectoryInfo() {
        return name + ":" + value;
    }

    @Override
    public void add(DirectoryInf branch) {
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
