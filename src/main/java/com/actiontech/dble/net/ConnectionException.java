/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

public class ConnectionException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final int code;
    private final String msg;

    public ConnectionException(int code, String msg) {
        super();
        this.code = code;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "ConnectionException [code=" + code + ", msg=" + msg + "]";
    }

}
