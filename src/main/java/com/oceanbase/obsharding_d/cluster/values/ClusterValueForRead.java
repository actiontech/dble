/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public class ClusterValueForRead<T> implements ClusterValue<T> {


    protected transient boolean converted = false;

    protected String instanceName;
    protected long createdAt;
    protected int apiVersion;


    protected transient T tmpData;
    protected JsonElement rawData;
    protected transient Class type;


    public ClusterValueForRead() {
    }

    public ClusterValueForRead(Class<T> type) {
        this.type = type;
    }


    @Override
    public <N> ClusterValue<N> convertTo(@Nonnull Class<N> newType) {
        this.type = newType;
        this.tmpData = null;
        return (ClusterValue<N>) this;
    }

    @Override
    @Nonnull
    public Class<T> getType() {
        assert type != null;
        return type;
    }


    @Override
    @Nonnull
    public String getInstanceName() {
        return instanceName;
    }

    @Override
    public long getCreatedAt() {
        return createdAt;
    }

    @Override
    public int getApiVersion() {
        return apiVersion;
    }


    /**
     * deserialize the raw data.
     *
     * @return
     */
    @Override
    @Nonnull
    public T getData() {
        assert type != null;
        if (!converted) {
            tmpData = getDataByType(getType());
            converted = true;
        }

        return tmpData;

    }


    @Override
    @Nonnull
    public String getRawDataStr() {
        assert type != null;
        String ret;
        if (rawData == null) {
            ret = null;
        } else {
            try {
                if (rawData.isJsonPrimitive()) {
                    final String str = rawData.getAsString();
                    ret = Strings.isBlank(str) ? null : str;
                } else {
                    ret = JsonFactory.getJson().toJson(rawData);
                }
            } catch (JsonParseException e) {
                LOGGER.debug("", e);
                throw new IllegalStateException("can't parse the value " + this, e);
            }
        }

        if (Strings.isEmpty(ret)) {
            throw new IllegalStateException("read null value of " + this);
        }
        return ret;

    }

    @Nonnull
    private <N> N getDataByType(Class<N> newClass) {
        assert type != null;
        N ret;
        if (rawData == null) {
            throw new IllegalStateException("read null value of " + this);
        }

        if (RawJson.class.isAssignableFrom(newClass)) {
            if (rawData.isJsonObject()) {
                return (N) RawJson.of((JsonObject) rawData);
            }
            throw new IllegalStateException("read illegal value of " + this);

        }
        if (SelfSerialize.class.isAssignableFrom(newClass)) {
            SelfSerialize<N> selfSerialize = null;
            try {
                selfSerialize = (SelfSerialize<N>) newClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {

                LOGGER.error("please provide default constructor for {}", newClass.getName());
                throw new IllegalStateException(e);
            }
            ret = selfSerialize.deserialize(rawData);

        } else {
            try {
                ret = JsonFactory.getJson().fromJson(rawData, newClass);
            } catch (JsonParseException e) {
                LOGGER.debug("", e);
                throw new IllegalStateException("can't parse the value " + this, e);
            }

        }

        return ret;
    }


    @Override
    public String toString() {
        return "ClusterValueForRead{" +
                "instanceName=" + instanceName +
                ", createdAt=" + createdAt +
                ", apiVersion=" + apiVersion +
                ", data=" + tmpData +
                ", converted=" + converted +
                ", rawData=" + rawData +
                '}';
    }
}
