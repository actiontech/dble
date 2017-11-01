/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

public final class ServerSptPrepare {
    private ServerConnection source;
    private Map<String, List<String>>sptPrepares;
    private List<String>sptArguments;
    private String sptStmt;
    private boolean isUserVar;
    private String name;

    public ServerSptPrepare(ServerConnection c) {
        this.source = c;
        this.sptPrepares = new HashMap<>();
        this.sptArguments = null;
        this.isUserVar = false;
        this.sptStmt = null;
        this.name = null;
    }

    public void setPrepare(String name, List<String>parts) {
        if (sptPrepares.containsKey(name)) {
            sptPrepares.replace(name, parts);
        } else {
            sptPrepares.put(name, parts);
        }
    }

    public List<String> getPrepare(String name) {
        if (sptPrepares.containsKey(name)) {
            return sptPrepares.get(name);
        } else {
            return null;
        }
    }

    public boolean delPrepare(String name) {
        if (sptPrepares.containsKey(name)) {
            sptPrepares.remove(name);
            return true;
        } else {
            return false;
        }
    }

    public void setExePrepare(String stmt, boolean userVar) {
        isUserVar = userVar;
        sptStmt = stmt;
    }

    public String getExePrepare() {
        if (isUserVar) {
            String key = "@" + sptStmt;
            return source.getUsrVariables().get(key);
        } else {
            return sptStmt;
        }
    }

    public boolean isUserVar() {
        return isUserVar;
    }

    public void setArguments(List<String> args) {
        sptArguments = args;
    }

    public List<String>getArguments() {
        return sptArguments;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
