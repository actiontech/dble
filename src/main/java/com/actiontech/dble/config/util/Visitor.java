/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.util;

/**
 * @author mycat
 */
public interface Visitor {

    void visit(String name, Class<?> type, Class<?> definedIn, Object value);

}
