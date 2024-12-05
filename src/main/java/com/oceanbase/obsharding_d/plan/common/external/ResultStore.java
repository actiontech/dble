/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.external;

import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;

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
