package io.mycat.config.loader.zkprocess.entity.server;

import io.mycat.config.loader.zkprocess.entity.Propertied;
import io.mycat.config.loader.zkprocess.entity.Property;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * 系统信息
 * 源文件名：System.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月16日
 * 修改作者：liujun
 * 修改日期：2016年9月16日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "system")
public class System implements Propertied {

    protected List<Property> property;

    public List<Property> getProperty() {
        if (this.property == null) {
            property = new ArrayList<>();
        }
        return property;
    }

    public void setProperty(List<Property> property) {
        this.property = property;
    }

    @Override
    public void addProperty(Property property) {
        this.getProperty().add(property);
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("System [property=");
        builder.append(property);
        builder.append("]");
        return builder.toString();
    }

}
