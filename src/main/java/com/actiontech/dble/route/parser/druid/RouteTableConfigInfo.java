package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;

/**
 * Created by szf on 2020/5/26.
 */
public class RouteTableConfigInfo {


    private String schema;
    private BaseTableConfig tableConfig;
    private String alias;
    private Object value;


    public RouteTableConfigInfo(String schema, BaseTableConfig tableConfig, String alias, Object value) {
        this.schema = schema;
        this.tableConfig = tableConfig;
        this.alias = alias;
        this.value = value;
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public BaseTableConfig getTableConfig() {
        return tableConfig;
    }

    public void setTableConfig(BaseTableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }


    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }


    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
