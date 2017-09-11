/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import java.util.List;

public interface SQLJobHandler {

    void onHeader(List<byte[]> fields);

    boolean onRowData(String dataNode, byte[] rowData);

    void finished(String dataNode, boolean failed);
}
