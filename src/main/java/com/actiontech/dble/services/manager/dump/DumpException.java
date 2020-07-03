package com.actiontech.dble.services.manager.dump;

public class DumpException extends RuntimeException {

    public DumpException() {
        super();
    }

    public DumpException(String message, Throwable cause) {
        super(message, cause);
    }

    public DumpException(String message) {
        super(message);
    }

    public DumpException(Throwable cause) {
        super(cause);
    }

}
