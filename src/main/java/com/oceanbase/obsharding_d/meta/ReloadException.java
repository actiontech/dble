/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta;

import com.oceanbase.obsharding_d.config.ErrorCode;

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
