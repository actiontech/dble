package com.actiontech.dble.manager.handler.dump.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DumpContent {

    private volatile boolean canDump = false;
    private volatile boolean isFooter = false;
    private List<String> stmts;
    protected int currentIndex = -1;
    protected Collection<String> dataNodes;

    public DumpContent() {
        stmts = new ArrayList<>(8);
    }

    public void setCanDump(boolean canDump) {
        this.canDump = canDump;
    }

    public boolean canDump() {
        return canDump;
    }

    public boolean isFooter() {
        return isFooter;
    }

    public void setFooter(boolean footer) {
        this.isFooter = footer;
    }

    public Collection<String> getDataNodes() {
        return dataNodes;
    }

    public void add(String stmt) {
        stmts.add(stmt);
    }

    public void replace(String stmt) {
        stmts.set(currentIndex, stmt);
    }

    public boolean hasNext() {
        ++currentIndex;
        if (currentIndex < stmts.size()) {
            return true;
        }
        currentIndex = -1;
        return false;
    }

    public String get() {
        return stmts.get(currentIndex);
    }

    public String get(String dataNode) {
        return get();
    }

    @Override
    public String toString() {
        return "dump file " + (isFooter ? "header " : "footer ");
    }

}
