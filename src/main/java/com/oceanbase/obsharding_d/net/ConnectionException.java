/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net;

public class ConnectionException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final int code;
    private final String msg;

    public ConnectionException(int code, String msg) {
        super(msg);
        this.code = code;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "ConnectionException [code=" + code + ", msg=" + msg + "]";
    }

}
