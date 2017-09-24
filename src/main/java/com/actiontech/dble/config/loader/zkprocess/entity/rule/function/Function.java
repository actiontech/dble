/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.rule.function;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;
import com.actiontech.dble.config.loader.zkprocess.entity.Propertied;
import com.actiontech.dble.config.loader.zkprocess.entity.Property;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * author:liujun
 * Created:2016/9/18
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "function")
public class Function implements Propertied, Named {


    @XmlAttribute(required = true)
    protected String name;

    @XmlAttribute(required = true, name = "class")
    protected String clazz;

    protected List<Property> property;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

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
    public void addProperty(Property prop) {
        this.getProperty().add(prop);
    }

    @Override
    public String toString() {
        String builder = "Function [name=" +
                name +
                ", clazz=" +
                clazz +
                ", property=" +
                property +
                "]";
        return builder;
    }


}
