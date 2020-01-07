/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;

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

    ReferenceHandlerInfo(String name, String type, String baseSQL, DMLResponseHandler handler) {
        this(name, type, handler);
        this.baseSQL = baseSQL;
    }

    ReferenceHandlerInfo(String name, String type, DMLResponseHandler handler) {
        this.name = name;
        this.type = type;
        this.handler = handler;
    }

    public String getRefOrSQL() {
        StringBuilder names = new StringBuilder("");
        for (String child : stepChildren) {
            if (names.length() > 0) {
                names.append("; ");
            }
            names.append(child);
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
