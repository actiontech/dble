/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.server;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;
import com.actiontech.dble.config.loader.zkprocess.entity.Propertied;
import com.actiontech.dble.config.loader.zkprocess.entity.Property;
import com.actiontech.dble.config.loader.zkprocess.entity.server.user.Privileges;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "user")
public class User implements Propertied, Named {

    @XmlAttribute(required = true)
    protected String name;

    protected List<Property> property;

    protected Privileges privileges;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Privileges getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Privileges privileges) {
        this.privileges = privileges;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("User{" + "name='").append(name).append('\'').append(", property=").append(property);
        if (privileges != null) {
            sb.append(", privileges=").append(privileges);
        }
        sb.append('}');
        return sb.toString();
    }

}
