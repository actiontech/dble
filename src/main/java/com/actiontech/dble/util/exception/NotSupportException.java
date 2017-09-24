/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util.exception;

public class NotSupportException extends RuntimeException {
    private static final long serialVersionUID = 7394431636300968222L;

    public NotSupportException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public NotSupportException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public NotSupportException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public NotSupportException(String errorCode) {
        super(errorCode);
    }

    public NotSupportException(Throwable cause) {
        super(cause);
    }

    public NotSupportException() {
        super("not support yet!");
    }
}
