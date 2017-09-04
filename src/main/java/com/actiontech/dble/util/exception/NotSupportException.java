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
