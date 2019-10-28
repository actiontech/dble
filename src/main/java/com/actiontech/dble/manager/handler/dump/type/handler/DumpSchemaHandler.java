package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.manager.handler.dump.type.DumpContent;
import com.actiontech.dble.manager.handler.dump.type.DumpSchema;

import java.util.HashMap;
import java.util.Map;

public class DumpSchemaHandler implements DumpHandler {

    public void handle(DumpContent content) {
        DumpSchema schema = (DumpSchema) content;
        Map<String, StringBuilder> dataNodeStmts = new HashMap<>();
        StringBuilder sb;
        String stmt;
        while (content.hasNext()) {
            stmt = schema.get();
            for (String dataNode : schema.getDataNodes()) {
                sb = dataNodeStmts.get(dataNode);
                String temp = stmt.replace("`" + schema.getSchema() + "`", "`" + schema.getDatabases().get(dataNode).getDatabase() + "`");
                if (sb == null) {
                    sb = new StringBuilder(temp);
                    dataNodeStmts.put(dataNode, sb);
                } else {
                    sb.append(temp);
                }
                sb.append(";");
            }
        }
        schema.setDataNodeStmts(dataNodeStmts);
    }

}
