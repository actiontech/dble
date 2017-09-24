/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

/**
 * FrontendQueryHandler
 *
 * @author mycat
 */
public interface FrontendQueryHandler {

    void query(String sql);

    void setReadOnly(Boolean readOnly);
}
