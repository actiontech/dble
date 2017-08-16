package io.mycat.sqlengine;

import java.util.List;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

public interface SQLJobHandler {

	void onHeader(List<byte[]> fields);

	boolean onRowData(String dataNode, byte[] rowData);

	void finished(String dataNode, boolean failed);
}
