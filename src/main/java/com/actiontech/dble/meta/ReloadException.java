package com.actiontech.dble.meta;

import com.actiontech.dble.config.ErrorCode;

public final class ReloadException extends RuntimeException {

    private final int errorCode;

    public ReloadException() {
        super();
        errorCode = ErrorCode.ER_YES;
    }

    public ReloadException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }


    public int getErrorCode() {
        return errorCode;
    }
}
