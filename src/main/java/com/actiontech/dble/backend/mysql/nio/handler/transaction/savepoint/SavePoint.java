package com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint;

import com.actiontech.dble.route.RouteResultsetNode;

import java.util.Collections;
import java.util.Set;

public class SavePoint {

    private volatile SavePoint prev;
    private Set<RouteResultsetNode> routeNodes;
    private String name;

    public SavePoint(String name) {
        this(name, Collections.emptySet());
    }

    public SavePoint(String name, Set<RouteResultsetNode> routeNodes) {
        this.name = name;
        this.routeNodes = routeNodes;
    }

    public Set<RouteResultsetNode> getRouteNodes() {
        return routeNodes;
    }

    public void setRouteNodes(Set<RouteResultsetNode> routeNodes) {
        this.routeNodes = routeNodes;
    }

    public String getName() {
        return name;
    }

    public SavePoint getPrev() {
        return prev;
    }

    public void setPrev(SavePoint prev) {
        this.prev = prev;
    }

}
