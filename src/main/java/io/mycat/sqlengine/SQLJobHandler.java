package io.mycat.sqlengine;

import java.util.List;

public interface SQLJobHandler {

	void onHeader(List<byte[]> fields);

	boolean onRowData(String dataNode, byte[] rowData);

	void finished(String dataNode, boolean failed);
}
