package io.mycat.config.loader.zkprocess.entity.server.user;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;
import io.mycat.config.loader.zkprocess.entity.Propertied;
import io.mycat.config.loader.zkprocess.entity.Property;
import io.mycat.config.loader.zkprocess.entity.server.user.privilege.Privileges;

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
    public void addProperty(Property property) {
        this.getProperty().add(property);
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
