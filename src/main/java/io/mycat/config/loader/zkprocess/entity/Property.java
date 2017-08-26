package io.mycat.config.loader.zkprocess.entity;

import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

/**
 *
 * author:liujun
 * Created:2016/9/16
 *
 *
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Property")
public class Property implements Named {

    @XmlValue
    protected String value;
    @XmlAttribute(name = "name")
    protected String name;

    public String getValue() {
        return value;
    }

    public Property setValue(String val) {
        this.value = val;
        return this;
    }

    public String getName() {
        return name;
    }

    public Property setName(String propName) {
        this.name = propName;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Property property = (Property) o;
        return value.equals(property.value) && name.equals(property.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, name);
    }

    @Override
    public String toString() {
        return "Property{" + "value='" + value + '\'' + ", name='" + name + '\'' + '}';
    }
}
