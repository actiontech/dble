/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.parse;

import com.oceanbase.obsharding_d.route.parser.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertQueryPos {


    public boolean isIgnore() {
        return isIgnore;
    }

    public void setIgnore(boolean ignore) {
        isIgnore = ignore;
    }

    public boolean isReplace() {
        return isReplace;
    }

    public void setReplace(boolean replace) {
        isReplace = replace;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public Map<String, Integer> getColNameIndexMap() {
        return colNameIndexMap;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Pair<Integer, Integer> getColumnRange() {
        return columnRange;
    }

    public void setColumnRange(Pair<Integer, Integer> columnRange) {
        this.columnRange = columnRange;
    }

    public List<Pair<Integer, Integer>> getValuesRange() {
        return valuesRange;
    }

    public List<List<Pair<Integer, Integer>>> getValueItemsRange() {
        return valueItemsRange;
    }

    public Pair<Integer, Integer> getQueryRange() {
        return queryRange;
    }

    public void setQueryRange(Pair<Integer, Integer> queryRange) {
        this.queryRange = queryRange;
    }

    public char[] getInsertChars() {
        if (this.insertChars == null) {
            this.insertChars = this.insertString.toCharArray();
        }
        return insertChars;
    }

    public void setInsertString(String insertString) {
        this.insertString = insertString;
    }

    public void clear() {
        insertChars = null;
        insertString = null;
        queryRange = null;
        valuesRange.clear();
        valueItemsRange.clear();
        columnRange = null;
        colNameIndexMap.clear();
        columns.clear();
    }

    private String insertString;
    private char[] insertChars;
    private Pair<Integer, Integer> queryRange;
    private final List<Pair<Integer, Integer>> valuesRange = new ArrayList<>();
    private final List<List<Pair<Integer, Integer>>> valueItemsRange = new ArrayList<>();
    private Pair<Integer, Integer> columnRange;
    private boolean isIgnore = false;
    private boolean isReplace = false;
    private String tableName;
    private final Map<String, Integer> colNameIndexMap = new HashMap<>();
    private final List<String> columns = new ArrayList<>();
}
