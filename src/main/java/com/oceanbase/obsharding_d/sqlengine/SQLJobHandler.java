/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;

import java.util.List;

public interface SQLJobHandler {

    void onHeader(List<byte[]> fields);

    void onRowData(byte[] rowData);

    void finished(String shardingNode, boolean failed);
}
