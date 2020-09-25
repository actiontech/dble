/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ServerSptPrepare {
    private ShardingService service;
    private Map<String, List<String>> sptPrepares;
    private List<String> sptArguments;
    private String sptStmt;
    private boolean isUserVar;
    private String name;

    public ServerSptPrepare(ShardingService service) {
        this.service = service;
        this.sptPrepares = new HashMap<>();
        this.sptArguments = null;
        this.isUserVar = false;
        this.sptStmt = null;
        this.name = null;
    }

    public void setPrepare(String name0, List<String> parts) {
        sptPrepares.put(name0, parts);
    }

    public List<String> getPrepare(String name0) {
        if (sptPrepares.containsKey(name0)) {
            return sptPrepares.get(name0);
        } else {
            return null;
        }
    }

    public boolean delPrepare(String name0) {
        if (sptPrepares.containsKey(name0)) {
            sptPrepares.remove(name0);
            return true;
        } else {
            return false;
        }
    }

    public void setExePrepare(String stmt, boolean userVar) {
        isUserVar = userVar;
        sptStmt = stmt;
    }

    /* In user variable, the string is primordial, so we have to truncate the quotes */
    private String getStmtFromUserVar() {
        String key = "@" + sptStmt;
        String stmt = service.getUsrVariables().get(key);
        String rstmt = null;

        if (stmt != null) {
            for (int i = 0; i < stmt.length(); i++) {
                char c1 = stmt.charAt(i);
                switch (c1) {
                    case '\'':
                    case '"':
                        int j = stmt.lastIndexOf(c1, stmt.length() - 1);
                        if (j != -1) {
                            rstmt = stmt.substring(++i, j);
                        }
                        return rstmt;
                    default:
                        break;
                }
            }
        }
        return rstmt;
    }

    public String getExePrepare() {
        if (isUserVar) {
            return getStmtFromUserVar();
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

    public List<String> getArguments() {
        return sptArguments;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
