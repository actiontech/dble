/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.route.RouteResultset;

/**
 * @author mycat
 */
public interface Session {

    /**
     * get frontend conn
     */
    FrontendConnection getSource();

    /**
     * get size of target conn
     */
    int getTargetCount();

    /**
     * execute session
     */
    void execute(RouteResultset rrs, int type);

    /**
     * commit session
     */
    void commit();

    /**
     * rollback session
     */
    void rollback();

    /**
     * terminated the session ,do it after close front conn
     */
    void terminate();

}
