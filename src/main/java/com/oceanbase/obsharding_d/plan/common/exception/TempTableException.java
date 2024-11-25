/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.exception;

public class TempTableException extends RuntimeException {

    private static final long serialVersionUID = 2869994979718401423L;

    public TempTableException(String message, Throwable cause) {
        super(message, cause);
    }

    public TempTableException(String message) {
        super(message);
    }

    public TempTableException(Throwable cause) {
        super(cause);
    }
}
