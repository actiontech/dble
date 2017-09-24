/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;

/**
 * ZkDataImpl
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class ZkDataImpl implements DataInf {

    private String name;

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
