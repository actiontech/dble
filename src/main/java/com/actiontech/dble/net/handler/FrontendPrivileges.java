/*
* Copyright (C) 2016-2017 ActionTech.
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
     */
    boolean schemaExists(String schema);

    /**
     */
    boolean userExists(String user, String host);

    /**
     */
    String getPassword(String user);

    /**
     * get Schemas of User
     */
    Set<String> getUserSchemas(String user);

    /**
     * @param user
     * @return
     */
    Boolean isReadOnly(String user);

    /**
     * get user Benchmark
     *
     * @param user
     * @return
     */
    int getBenchmark(String user);


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
     * @return
     */
    boolean checkFirewallSQLPolicy(String user, String sql);
}
