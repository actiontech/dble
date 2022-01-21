/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
