/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.sequence.handler;

import java.sql.SQLNonTransientException;

/**
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @version 1.0
 * @time Create on 2013-12-20 15:35:53
 */
public interface SequenceHandler {

    long nextId(String prefixName) throws SQLNonTransientException;

    void load(boolean isLowerCaseTableNames);

}
