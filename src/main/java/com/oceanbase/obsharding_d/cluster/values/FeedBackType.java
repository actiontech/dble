/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import java.util.Objects;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public final class FeedBackType {
    public static final FeedBackType SUCCESS = new FeedBackType(true, null);
    boolean success;
    String message;


    private FeedBackType() {
    }

    private FeedBackType(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public FeedBackType setSuccess(boolean successTmp) {
        this.success = successTmp;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public FeedBackType setMessage(String messageTmp) {
        this.message = messageTmp;
        return this;
    }


    public static FeedBackType ofSuccess(String msg) {
        return new FeedBackType(true, msg);
    }

    public static FeedBackType ofError(String errorMsg) {
        return new FeedBackType(false, errorMsg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeedBackType)) return false;
        FeedBackType that = (FeedBackType) o;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }

    @Override
    public String toString() {
        return "LockType{" +
                "message='" + message + '\'' +
                '}';
    }
}
