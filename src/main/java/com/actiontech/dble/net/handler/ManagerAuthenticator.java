/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;

/**
 * ManagerAuthenticator
 *
 * @author mycat
 */
public class ManagerAuthenticator extends FrontendAuthenticator {
    public ManagerAuthenticator(FrontendConnection source) {
        super(source);
    }

    @Override
    protected NIOHandler successCommendHander() {
        return new ManagerCommandHandler(source);
    }
}
