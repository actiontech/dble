/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.server;

import com.actiontech.dble.config.loader.zkprocess.entity.Propertied;
import com.actiontech.dble.config.loader.zkprocess.entity.Property;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * author:liujun
 * Created:2016/9/16
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
    public void addProperty(Property prop) {
        this.getProperty().add(prop);
    }


    @Override
    public String toString() {
        String builder = "System [property=" +
                property +
                "]";
        return builder;
    }

}
