/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.JsonParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public interface ClusterValue<T> {


    Logger LOGGER = LogManager.getLogger(ClusterValue.class);


    static <T> ClusterValueForBaseWrite<T> constructForWrite(T data, int apiVersion) {
        if (data == null) {
            //cluster value can't be null.
            //if you are cleaning key-value,use cleanKv instead.
            //if you are creating an key with empty value. Using new Empty() or other initial value
            throw new IllegalArgumentException("cluster value can't be null");
        }
        if (data instanceof RawJson) {
            return new ClusterValueForRawWrite<>(((RawJson) data).getJsonObject(), apiVersion);
        }
        return new ClusterValueForWrite<>(data, apiVersion);
    }


    /**
     * @param value
     * @param <T>
     * @return
     * @throws JsonProcessingException throws when the value can't parse ,maybe you the content created by old OBsharding-D.
     */
    static <T> ClusterValue<T> readFromJson(String value, Class<T> tClass) {
        if (Strings.isEmpty(value)) {
            return empty();
        }
        try {

            return (ClusterValue<T>) JsonFactory.getJson().fromJson(value, ClusterValueForRead.class).convertTo(tClass);
        } catch (JsonParseException e) {
            throw new RuntimeException("can't parse the illegal value " + value, e);
        }
    }

    static <T> ClusterValue<T> empty() {
        return new ClusterValueForRead<>();
    }

    <N> ClusterValue<N> convertTo(@Nonnull Class<N> newType);

    @Nonnull
    Class<T> getType();

    @Nonnull
    String getInstanceName();

    long getCreatedAt();

    int getApiVersion();

    @Nonnull
    T getData();

    @Nonnull
    String getRawDataStr();
}
