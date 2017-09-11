/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.google.common.base.Joiner;

import java.io.Serializable;
import java.sql.Types;
import java.util.*;

/**
 * Created by magicdoom on 2016/3/24.
 * <p>
 * <p>
 * 1.no return
 * <p>
 * ok
 * <p>
 * <p>
 * 2.simple
 * <p>
 * ok
 * row
 * eof
 * <p>
 * <p>
 * 3.list
 * <p>
 * <p>
 * row
 * row
 * row
 * row
 * eof
 * ok
 */
public class Procedure implements Serializable {
    private String originSql;
    private String name;
    private String callSql;
    private String setSql;
    private String selectSql;
    private Set<String> selectColumns = new LinkedHashSet<>();
    private Set<String> listFields = new LinkedHashSet<>();
    private boolean isResultList = false;

    public boolean isResultList() {
        return isResultList;
    }

    public boolean isResultSimpleValue() {
        return selectSql != null && !isResultList;
    }

    public boolean isResultNothing() {
        return selectSql == null && !isResultList;
    }

    public void setResultList(boolean resultList) {
        isResultList = resultList;
    }

    public String toPreCallSql(String dbType) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ call ");
        sb.append(this.getName()).append("(");
        Collection<ProcedureParameter> parameters = this.getParameterMap().values();
        int j = 0;
        for (ProcedureParameter parameter : parameters) {

            String strParameter = "?";
            String joinStr = j == this.getParameterMap().size() - 1 ? strParameter : strParameter + ",";
            sb.append(joinStr);
            j++;
        }
        sb.append(")}");
        return sb.toString();
    }

    public String toChangeCallSql(String dbType) {
        StringBuilder sb = new StringBuilder();
        sb.append("call ");
        sb.append(this.getName()).append("(");
        Collection<ProcedureParameter> parameters = this.getParameterMap().values();
        int j = 0;
        for (ProcedureParameter parameter : parameters) {
            Object value = parameter.getValue() != null && Types.VARCHAR == parameter.getJdbcType() ? "'" + parameter.getValue() + "'" : parameter.getValue();
            String strParameter = parameter.getValue() == null ? parameter.getName() : String.valueOf(value);
            String joinStr = j == this.getParameterMap().size() - 1 ? strParameter : strParameter + ",";
            sb.append(joinStr);
            j++;
        }
        sb.append(")");
        if (isResultSimpleValue()) {
            sb.append(";select ");
            sb.append(Joiner.on(",").join(selectColumns));
        }
        return sb.toString();
    }

    public Set<String> getListFields() {
        return listFields;
    }

    public void setListFields(Set<String> listFields) {
        this.listFields = listFields;
    }

    public Set<String> getSelectColumns() {
        return selectColumns;
    }

    public String getSetSql() {
        return setSql;
    }

    public void setSetSql(String setSql) {
        this.setSql = setSql;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    private Map<String, ProcedureParameter> parameterMap = new LinkedHashMap<>();

    public String getOriginSql() {
        return originSql;
    }

    public void setOriginSql(String originSql) {
        this.originSql = originSql;
    }

    public Map<String, ProcedureParameter> getParameterMap() {
        return parameterMap;
    }

    public void setParameterMap(Map<String, ProcedureParameter> parameterMap) {
        this.parameterMap = parameterMap;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCallSql() {
        return callSql;
    }

    public void setCallSql(String callSql) {
        this.callSql = callSql;
    }
}
