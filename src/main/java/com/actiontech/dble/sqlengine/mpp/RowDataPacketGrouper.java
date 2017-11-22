/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * implement group function select a,count(*),sum(*) from A group by a
 *
 * @author wuzhih
 */
public class RowDataPacketGrouper {

    private List<RowDataPacket> result = Collections.synchronizedList(new ArrayList<RowDataPacket>());
    private final int[] groupColumnIndexes;

    public RowDataPacketGrouper(int[] groupColumnIndexes) {
        super();
        this.groupColumnIndexes = groupColumnIndexes;
    }

    public List<RowDataPacket> getResult() {
        return result;
    }

    public void addRow(RowDataPacket rowDataPkg) {
        for (RowDataPacket row : result) {
            if (sameGroupColumns(rowDataPkg, row)) {
                return;
            }
        }

        // not aggreated ,insert new
        result.add(rowDataPkg);

    }


    // private static final

    private boolean sameGroupColumns(RowDataPacket newRow, RowDataPacket existRow) {
        if (groupColumnIndexes == null) { // select count(*) from aaa , or group
            // column
            return true;
        }
        for (int groupColumnIndex : groupColumnIndexes) {
            if (!Arrays.equals(newRow.fieldValues.get(groupColumnIndex),
                    existRow.fieldValues.get(groupColumnIndex))) {
                return false;
            }

        }
        return true;

    }
}
