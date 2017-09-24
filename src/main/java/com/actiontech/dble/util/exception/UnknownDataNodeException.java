/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util.exception;

/**
 * @author mycat
 */
public class UnknownDataNodeException extends RuntimeException {
    private static final long serialVersionUID = -3752985849571697432L;

    public UnknownDataNodeException() {
        super();
    }

    public UnknownDataNodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownDataNodeException(String message) {
        super(message);
    }

    public UnknownDataNodeException(Throwable cause) {
        super(cause);
    }

}
