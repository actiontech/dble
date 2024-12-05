/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump;

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
