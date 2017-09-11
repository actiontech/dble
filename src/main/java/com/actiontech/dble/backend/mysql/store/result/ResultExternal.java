/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.result;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.List;

public interface ResultExternal {
    /**
     * Reset the current position of this object.
     */
    void reset();

    /**
     * Get the next row from the result.
     *
     * @return the next row or null
     */
    RowDataPacket next();

    /**
     * Add a row to this object.
     *
     * @param values the row to add
     * @return the new number of rows in this object
     */
    int addRow(RowDataPacket row);

    /**
     * Add a number of rows to the result.
     *
     * @param rows the list of rows to add
     * @return the new number of rows in this object
     */
    int addRows(List<RowDataPacket> rows);

    /**
     * This method is called after all rows have been added.
     */
    void done();

    /**
     * Close this object and delete the temporary file.
     */
    void close();

    /**
     * Remove the row with the given values from this object if such a row
     * exists.
     *
     * @param values the row
     * @return the new row count
     */
    int removeRow(RowDataPacket row);

    /**
     * Check if the given row exists in this object.
     *
     * @param values the row
     * @return true if it exists
     */
    boolean contains(RowDataPacket row);

    /**
     * Create a shallow copy of this object if possible.
     *
     * @return the shallow copy, or null
     */
    ResultExternal createShallowCopy();

    /**
     * count tapes split by resultExternal
     *
     * @return tape's count
     */
    int tapeCount();
}
