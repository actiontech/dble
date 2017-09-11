/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util.exception;

/**
 * @author mycat
 */
public class HeartbeatException extends RuntimeException {
    private static final long serialVersionUID = 7639414445868741580L;

    public HeartbeatException() {
        super();
    }

    public HeartbeatException(String message, Throwable cause) {
        super(message, cause);
    }

    public HeartbeatException(String message) {
        super(message);
    }

    public HeartbeatException(Throwable cause) {
        super(cause);
    }

}
