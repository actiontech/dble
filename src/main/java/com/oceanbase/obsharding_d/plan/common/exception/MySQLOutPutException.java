/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.exception;

import com.oceanbase.obsharding_d.util.StringUtil;

/**
 * MySQLOutPutException
 *
 * @author oceanbase
 */
public class MySQLOutPutException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = -7089907072181836842L;

    private final int errorCode;
    private final String sqlState;

    public MySQLOutPutException(int errorCode, String sqlState, String msg) {
        super(msg);
        this.errorCode = errorCode;
        if (StringUtil.isEmpty(sqlState)) {
            this.sqlState = "HY000";
        } else {
            this.sqlState = sqlState;
        }
    }

    public MySQLOutPutException(int errorCode, String sqlState, String msg, Throwable cause) {
        super(msg, cause);
        this.errorCode = errorCode;
        if (StringUtil.isEmpty(sqlState)) {
            this.sqlState = "HY000";
        } else {
            this.sqlState = sqlState;
        }
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String toMysqlErrorMsg() {
        return String.format("ERROR %d (%s): %s", errorCode, sqlState, getMessage());
    }

}
