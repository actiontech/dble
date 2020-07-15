/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user;

import com.actiontech.dble.cluster.zkprocess.entity.Named;
import com.actiontech.dble.cluster.zkprocess.entity.Propertied;
import com.actiontech.dble.cluster.zkprocess.entity.Property;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huqing.yan on 2017/6/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "blacklist")
public class BlackList implements Propertied, Named {
    @XmlAttribute(required = true)
    protected String name;
    protected List<Property> property;

    public String getName() {
        return name;
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
        return "BlackList {name=" + name + ",property=[" + property + "]}";
    }
}
