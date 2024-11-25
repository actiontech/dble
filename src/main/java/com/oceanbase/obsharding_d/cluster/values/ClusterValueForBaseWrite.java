/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public class ClusterValueForBaseWrite<T> {

    protected static String instanceName;
    protected long createdAt;
    protected int apiVersion;
    protected transient T data;
    protected transient JsonElement rawData;

    static {
        instanceName = SystemConfig.getInstance().getInstanceName();
        if (instanceName == null) {
            throw new IllegalStateException("please specify the instanceName");
        }
    }

    ClusterValueForBaseWrite(T data, int apiVersion) {
        this.createdAt = System.currentTimeMillis();
        this.apiVersion = apiVersion;
        this.data = data;
    }

    ClusterValueForBaseWrite(JsonElement rawData, int apiVersion) {
        this.createdAt = System.currentTimeMillis();
        this.apiVersion = apiVersion;
        this.rawData = rawData;
    }

    public String toJson() {
        try {
            return JsonFactory.getJson().toJson(this);
        } catch (JsonParseException e) {
            throw new RuntimeException("can't serialize the value " + this);
        }

    }


    public String getInstanceName() {
        return instanceName;
    }


    public long getCreatedAt() {
        return createdAt;
    }


    public int getApiVersion() {
        return apiVersion;
    }


    public T getData() {
        return data;
    }


    public JsonElement getRawData() {
        return rawData;
    }


    @Override
    public String toString() {
        return "ClusterValueForBaseWrite{" +
                "instanceName='" + instanceName + '\'' +
                ", createdAt=" + createdAt +
                ", apiVersion=" + apiVersion +
                ", data=" + data +
                ", rawData=" + rawData +
                '}';
    }
}
