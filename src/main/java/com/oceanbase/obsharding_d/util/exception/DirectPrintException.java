/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util.exception;

/**
 * @author dcy
 * Create Date: 2021-08-31
 */
public class DirectPrintException extends RuntimeException {
    public DirectPrintException(String message) {
        super(message);
    }

    public DirectPrintException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirectPrintException(Throwable cause) {
        super(cause);
    }

    public DirectPrintException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
