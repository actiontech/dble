package io.mycat.config.model;

import java.util.HashMap;

/**
 * support[`] in table
 *
 * @author BEN GONG
 */
public class TableConfigMap extends HashMap<String, TableConfig> {

    private static final long serialVersionUID = -6605226933829917213L;

    @Override
    public TableConfig get(Object key) {
        String tableName = key.toString();
        if (tableName.contains("`")) {
            tableName = tableName.replaceAll("`", "");
        }

        return super.get(tableName);
    }

    @Override
    public boolean containsKey(Object key) {
        String tableName = key.toString();
        if (tableName.contains("`")) {
            tableName = tableName.replaceAll("`", "");
        }

        return super.containsKey(tableName);
    }
}
