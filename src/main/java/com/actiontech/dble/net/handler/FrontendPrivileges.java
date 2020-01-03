/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.handler;

import java.util.Set;

/**
 * FrontendPrivileges
 *
 * @author mycat
 */
public interface FrontendPrivileges {

    /**
     * check whether the schema exists
     *
     * @param schema
     */
    boolean schemaExists(String schema);

    /**
     * check whether the user exists
     *
     * @param user
     * @param host
     * @return
     */
    boolean userExists(String user, String host);

    /**
     * get user password
     *
     * @param user
     * @return
     */
    String getPassword(String user);

    /**
     * get Schemas of User
     *
     * @param user
     * @return
     */
    Set<String> getUserSchemas(String user);

    /**
     * check whether the user is read-only
     *
     * @param user
     * @return
     */
    boolean isReadOnly(String user);

    /**
     * check whether the user is manager
     *
     * @param user
     * @return
     */
    boolean isManagerUser(String user);

    /**
     * get user maxCon
     *
     * @param user
     * @return
     */
    int getMaxCon(String user);

    /**
     * checkFirewallWhiteHostPolicy
     *
     * @param user
     * @param host
     * @return
     */
    boolean checkFirewallWhiteHostPolicy(String user, String host);

    /**
     * checkFirewallSQLPolicy
     *
     * @param sql
     * @param user
     * @return
     */
    boolean checkFirewallSQLPolicy(String user, String sql);
}
