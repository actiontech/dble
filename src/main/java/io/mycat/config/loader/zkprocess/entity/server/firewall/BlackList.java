package io.mycat.config.loader.zkprocess.entity.server.firewall;

import io.mycat.config.loader.zkprocess.entity.Propertied;
import io.mycat.config.loader.zkprocess.entity.Property;

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
public class BlackList implements Propertied {
    @XmlAttribute(required = true)
    protected Boolean check;
    protected List<Property> property;

    public Boolean getCheck() {
        return check;
    }

    public void setCheck(Boolean check) {
        this.check = check;
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


    @Override
    public String toString() {
        return "BlackList {check=" + check + ",property=[" + property + "]}";
    }
}
