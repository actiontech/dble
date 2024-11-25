/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.util;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;

import java.util.LinkedHashSet;
import java.util.Set;

public class ReferenceHandlerInfo {
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public DMLResponseHandler getHandler() {
        return handler;
    }

    private String name;
    private String type;
    private String baseSQL;
    private Set<String> children = new LinkedHashSet<>();
    private Set<String> stepChildren = new LinkedHashSet<>();
    private final DMLResponseHandler handler;
    private boolean isIndentation;

    ReferenceHandlerInfo(String name, String type, String baseSQL, DMLResponseHandler handler) {
        this(name, type, handler);
        this.baseSQL = baseSQL;
    }

    public ReferenceHandlerInfo(String name, String type, String baseSQL, DMLResponseHandler handler, boolean isIndentation) {
        this.name = name;
        this.type = type;
        this.baseSQL = baseSQL;
        this.handler = handler;
        this.isIndentation = isIndentation;
    }

    ReferenceHandlerInfo(String name, String type, DMLResponseHandler handler) {
        this.name = name;
        this.type = type;
        this.handler = handler;
    }

    public ReferenceHandlerInfo(String name, String type, DMLResponseHandler handler, boolean isIndentation) {
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.isIndentation = isIndentation;
    }

    public String getRefOrSQL() {
        StringBuilder names = new StringBuilder("");
        for (String child : stepChildren) {
            if (!child.contains(ComplexQueryPlanUtil.TYPE_UPDATE_SUB_QUERY.toLowerCase())) {
                if (names.length() > 0) {
                    names.append("; ");
                }
                names.append(child);
            }
        }
        for (String child : children) {
            if (names.length() > 0) {
                names.append("; ");
            }
            names.append(child);
        }

        if (baseSQL != null) {
            if (names.length() > 0) {
                names.append("; ");
            }
            names.append(baseSQL);
        }
        return names.toString().replaceAll("[\\t\\n\\r]", " ");
    }

    public Set<String> getChildren() {
        return children;
    }

    public boolean isNestLoopQuery() {
        return this.stepChildren.size() != 0;
    }

    public boolean isIndentation() {
        return isIndentation;
    }

    void addAllStepChildren(Set<String> dependencies) {
        this.stepChildren.addAll(dependencies);
    }

    void addChild(String child) {
        this.children.add(child);
    }

    public void setBaseSQL(String baseSQL) {
        this.baseSQL = baseSQL;
    }
}
