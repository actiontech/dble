package com.actiontech.dble.plan;

import com.actiontech.dble.DbleServer;

/**
 * Created by szf on 2019/10/25.
 */
public class NamedFieldDetail {
    private final String schema;
    private final String name;
    private final int hashCode;

    public NamedFieldDetail(String inputSchema, String name) {
        String tempTableSchmea;
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tempTableSchmea = inputSchema == null ? null : inputSchema.toLowerCase();
        } else {
            tempTableSchmea = inputSchema;
        }
        this.schema = tempTableSchmea;
        this.name = name;

        //init hashCode
        int prime = 2;
        int hash = tempTableSchmea == null ? 0 : tempTableSchmea.hashCode();
        this.hashCode = hash * prime + (name == null ? 0 : name.toLowerCase().hashCode());
    }


    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }
}
