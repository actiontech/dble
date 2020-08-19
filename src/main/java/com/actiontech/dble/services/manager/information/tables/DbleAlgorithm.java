/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.route.function.*;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DbleAlgorithm extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_algorithm";

    private static final String COLUMN_NAME = "name";

    private static final String COLUMN_KEY = "key";

    private static final String COLUMN_VALUE = "value";

    private static final String COLUMN_IS_FILE = "is_file";

    private static final String KEY_CLASS = "class";

    public DbleAlgorithm() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_KEY, new ColumnMeta(COLUMN_KEY, "varchar(64)", false, true));
        columnsType.put(COLUMN_KEY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VALUE, new ColumnMeta(COLUMN_VALUE, "text", false));
        columnsType.put(COLUMN_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_IS_FILE, new ColumnMeta(COLUMN_IS_FILE, "varchar(5)", false));
        columnsType.put(COLUMN_IS_FILE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        List<String> nameList = Lists.newArrayList();
        Map<String, AbstractPartitionAlgorithm> functionMap = DbleServer.getInstance().getConfig().getFunctions();
        functionMap.forEach((key1, algorithm) -> {
            LinkedHashMap<String, String> classMap = Maps.newLinkedHashMap();
            classMap.put(COLUMN_NAME, algorithm.getName());
            classMap.put(COLUMN_KEY, KEY_CLASS);
            classMap.put(COLUMN_VALUE, algorithm.getClass().getName());
            classMap.put(COLUMN_IS_FILE, Boolean.FALSE.toString());
            rowList.add(classMap);
            nameList.add(algorithm.getName() + "-" + KEY_CLASS);
            Map<String, String> allPropertiesMap = algorithm.getAllProperties();
            allPropertiesMap.entrySet().stream().filter(properties -> !nameList.contains(algorithm.getName() + "-" + properties.getKey())).forEach(properties -> {
                String value = properties.getValue();
                String key = properties.getKey();
                LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                map.put(COLUMN_NAME, algorithm.getName());
                map.put(COLUMN_KEY, key);
                if (null != key && key.equalsIgnoreCase("mapFile") && value.length() > 1024) {
                    fillValue(algorithm, map);
                    map.put(COLUMN_IS_FILE, Boolean.TRUE.toString());
                } else {
                    map.put(COLUMN_VALUE, value);
                    map.put(COLUMN_IS_FILE, Boolean.FALSE.toString());
                }
                rowList.add(map);
                nameList.add(algorithm.getName() + "-" + key);
            });
        });
        return rowList;
    }

    private void fillValue(AbstractPartitionAlgorithm algorithm, LinkedHashMap<String, String> map) {
        AlgorithmType algorithmType = distinguishType(algorithm);
        switch (algorithmType) {
            case PARTITION_PATTERN:
                PartitionByPattern partitionByPattern = (PartitionByPattern) algorithm;
                map.put(COLUMN_VALUE, partitionByPattern.getMapFile());
                break;
            case PARTITION_FILE_MAP:
                PartitionByFileMap partitionByFileMap = (PartitionByFileMap) algorithm;
                map.put(COLUMN_VALUE, partitionByFileMap.getMapFile());
                break;
            case AUTO_PARTITION_LONG:
                AutoPartitionByLong autoPartitionByLong = (AutoPartitionByLong) algorithm;
                map.put(COLUMN_VALUE, autoPartitionByLong.getMapFile());
                break;
            case NO:
            default:
                break;
        }
    }

    protected static AlgorithmType distinguishType(AbstractPartitionAlgorithm algorithm) {
        if (algorithm == null) {
            return AlgorithmType.NO;
        } else if (algorithm instanceof AutoPartitionByLong) {
            return AlgorithmType.AUTO_PARTITION_LONG;
        } else if (algorithm instanceof PartitionByFileMap) {
            return AlgorithmType.PARTITION_FILE_MAP;
        } else if (algorithm instanceof PartitionByPattern) {
            return AlgorithmType.PARTITION_PATTERN;
        } else {
            return AlgorithmType.NO;
        }
    }

    public enum AlgorithmType {
        AUTO_PARTITION_LONG, PARTITION_FILE_MAP, PARTITION_PATTERN, NO
    }

}
