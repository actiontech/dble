/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * <readHost host="" url="" password="" user=""></readHost>
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "writeHost")
public class WriteHost {

    @XmlAttribute(required = true)
    protected String host;
    @XmlAttribute(required = true)
    protected String url;
    @XmlAttribute(required = true)
    protected String password;
    @XmlAttribute(required = true)
    protected String user;
    @XmlAttribute
    protected String usingDecrypt;
    @XmlAttribute
    protected String disabled;
    @XmlAttribute
    protected String id;
    @XmlAttribute
    protected String weight;

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }


    private List<ReadHost> readHost;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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


    public List<ReadHost> getReadHost() {
        if (this.readHost == null) {
            readHost = new ArrayList<>();
        }
        return readHost;
    }

    public void setReadHost(List<ReadHost> readHost) {
        this.readHost = readHost;
    }

    @Override
    public String toString() {
        return "WriteHost [host=" +
                host +
                ", url=" +
                url +
                ", password=" +
                password +
                ", user=" +
                user +
                ", disabled=" +
                disabled +
                ", id=" +
                id +
                ", usingDecrypt=" +
                usingDecrypt +
                ", weight=" +
                weight +
                ", readHost=" +
                readHost +
                "]";
    }

}
