/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.dbGroups;

import com.actiontech.dble.cluster.zkprocess.entity.Propertied;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.google.gson.annotations.Expose;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dbInstance")
public class DBInstance implements Propertied {

    @XmlAttribute(required = true)
    @Expose
    protected String name;
    @XmlAttribute(required = true)
    @Expose
    protected String url;
    @XmlAttribute(required = true)
    @Expose
    protected String password;
    @XmlAttribute(required = true)
    @Expose
    protected String user;
    @XmlAttribute(required = true)
    @Expose
    protected Integer maxCon;
    @XmlAttribute(required = true)
    @Expose
    protected Integer minCon;
    @XmlAttribute
    @Expose
    protected String usingDecrypt;
    @XmlAttribute
    @Expose
    protected String disabled;
    @XmlAttribute
    @Expose
    protected String id;
    @XmlAttribute
    @Expose
    protected String readWeight;

    @XmlAttribute
    @Expose
    protected Boolean primary;

    @Expose
    protected List<Property> property;

    @XmlTransient
    @Expose(serialize = false, deserialize = false)
    protected String dbGroup;

    public DBInstance() {
    }

    public DBInstance(String name, String url, String password, String user, Integer maxCon, Integer minCon, String disabled, String id, String readWeight,
                      Boolean primary, List<Property> property, String usingDecrypt) {
        this.name = name;
        this.url = url;
        this.password = password;
        this.user = user;
        this.maxCon = maxCon;
        this.minCon = minCon;
        this.disabled = disabled;
        this.id = id;
        this.readWeight = readWeight;
        this.primary = primary;
        this.property = property;
        this.usingDecrypt = usingDecrypt;
    }

    @Override
    public void addProperty(Property prop) {
        this.getProperty().add(prop);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUsingDecrypt() {
        return usingDecrypt;
    }

    public void setUsingDecrypt(String usingDecrypt) {
        this.usingDecrypt = usingDecrypt;
    }


    public String getDisabled() {
        return disabled;
    }

    public void setDisabled(String disabled) {
        this.disabled = disabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(Integer maxCon) {
        this.maxCon = maxCon;
    }

    public Integer getMinCon() {
        return minCon;
    }

    public void setMinCon(Integer minCon) {
        this.minCon = minCon;
    }


    public Boolean getPrimary() {
        return primary;
    }

    public void setPrimary(Boolean primary) {
        this.primary = primary;
    }


    public String getReadWeight() {
        return readWeight;
    }

    public void setReadWeight(String readWeight) {
        this.readWeight = readWeight;
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

    public void setDbGroup(String dbGroup) {
        this.dbGroup = dbGroup;
    }

    public String getDbGroup() {
        return dbGroup;
    }

    @Override
    public String toString() {
        return "dbInstance [name=" +
                name +
                ", url=" +
                url +
                ", password=" +
                password +
                ", user=" +
                user +
                ", maxCon=" +
                maxCon +
                ", minCon=" +
                minCon +
                ", disabled=" +
                disabled +
                ", id=" +
                id +
                ", usingDecrypt=" +
                usingDecrypt +
                ", weight=" +
                readWeight + ",property=" +
                property +
                "]";
    }
}
