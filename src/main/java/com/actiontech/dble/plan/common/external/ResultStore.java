/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.external;

import com.actiontech.dble.net.mysql.RowDataPacket;

public interface ResultStore {
    /* add a new row */
    void add(RowDataPacket row);

    /* all rows added */
    void done();

    /* visit all rows in the store */
    RowDataPacket next();

    int getRowCount();

    /* close result */
    void close();

    /* clear data */
    void clear();
}
