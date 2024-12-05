/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util.exception;

/**
 * @author dcy
 * Create Date: 2021-08-27
 */
public class NeedDelayedException extends RuntimeException {
    public NeedDelayedException() {
    }

    public NeedDelayedException(String message) {
        super(message);
    }

    public NeedDelayedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NeedDelayedException(Throwable cause) {
        super(cause);
    }

    public NeedDelayedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
