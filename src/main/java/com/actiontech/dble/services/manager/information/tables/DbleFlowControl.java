package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DbleFlowControl extends ManagerBaseTable {
    private static final String TABLE_NAME = "dble_flow_control";

    private static final String COLUMN_CONNECTION_TYPE = "connection_type";
    private static final String COLUMN_CONNECTION_ID = "connection_id";
    private static final String COLUMN_CONNECTION_INFO = "connection_info";
    private static final String COLUMN_WRITING_QUEUE_BYTES = "writing_queue_bytes";
    private static final String COLUMN_READING_QUEUE_BYTES = "reading_queue_bytes";
    private static final String COLUMN_FLOW_CONTROLLED = "flow_controlled";

    public DbleFlowControl() {
        super(TABLE_NAME, 6);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_CONNECTION_TYPE, new ColumnMeta(COLUMN_CONNECTION_TYPE, "varchar(15)", false, true));
        columnsType.put(COLUMN_CONNECTION_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONNECTION_ID, new ColumnMeta(COLUMN_CONNECTION_ID, "int(11)", false, true));
        columnsType.put(COLUMN_CONNECTION_ID, Fields.FIELD_TYPE_LONGLONG);

        columns.put(COLUMN_CONNECTION_INFO, new ColumnMeta(COLUMN_CONNECTION_INFO, "varchar(255)", false, false));
        columnsType.put(COLUMN_CONNECTION_INFO, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_WRITING_QUEUE_BYTES, new ColumnMeta(COLUMN_WRITING_QUEUE_BYTES, "int(11)", false, false));
        columnsType.put(COLUMN_WRITING_QUEUE_BYTES, Fields.FIELD_TYPE_LONGLONG);

        columns.put(COLUMN_READING_QUEUE_BYTES, new ColumnMeta(COLUMN_READING_QUEUE_BYTES, "int(11)", true, false));
        columnsType.put(COLUMN_READING_QUEUE_BYTES, Fields.FIELD_TYPE_LONGLONG);

        columns.put(COLUMN_FLOW_CONTROLLED, new ColumnMeta(COLUMN_FLOW_CONTROLLED, "varchar(7)", false, false));
        columnsType.put(COLUMN_FLOW_CONTROLLED, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rows = new ArrayList<>();
        {
            IOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
            for (IOProcessor p : processors) {
                //find all front connection
                for (FrontendConnection fc : p.getFrontends().values()) {
                    AbstractService service = fc.getService();
                    if (service instanceof ShardingService || service instanceof RWSplitService) {
                        int size = fc.getWritingSize().get();
                        LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                        row.put(COLUMN_CONNECTION_TYPE, "ServerConnection");
                        row.put(COLUMN_CONNECTION_ID, Long.toString(fc.getId()));
                        row.put(COLUMN_CONNECTION_INFO, fc.getHost() + ":" + fc.getLocalPort() + "/" + ((FrontendService) service).getSchema() + " user = " + ((FrontendService) service).getUser());
                        row.put(COLUMN_WRITING_QUEUE_BYTES, Integer.toString(size));
                        row.put(COLUMN_READING_QUEUE_BYTES, null);
                        row.put(COLUMN_FLOW_CONTROLLED, fc.isFrontWriteFlowControlled() ? "true" : "false");
                        rows.add(row);
                    }
                }
            }
        }
        {
            //find all mysql connection
            IOProcessor[] processors = DbleServer.getInstance().getBackendProcessors();
            for (IOProcessor p : processors) {
                for (BackendConnection bc : p.getBackends().values()) {
                    MySQLResponseService mc = bc.getBackendService();
                    if (mc == null) {
                        continue;
                    }
                    int writeSize = mc.getConnection().getWritingSize().get();
                    int readSize = mc.getReadSize();
                    LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                    row.put(COLUMN_CONNECTION_TYPE, "MySQLConnection");
                    row.put(COLUMN_CONNECTION_ID, Long.toString(mc.getConnection().getId()));
                    row.put(COLUMN_CONNECTION_INFO, mc.getConnection().getInstance().getConfig().getUrl() + "/" + mc.getSchema() + " mysqlId = " + mc.getConnection().getThreadId());
                    row.put(COLUMN_WRITING_QUEUE_BYTES, Integer.toString(writeSize));
                    row.put(COLUMN_READING_QUEUE_BYTES, Integer.toString(readSize));
                    row.put(COLUMN_FLOW_CONTROLLED, mc.isFlowControlled() ? "true" : "false");
                    rows.add(row);
                }
            }
        }
        return rows;
    }
}
