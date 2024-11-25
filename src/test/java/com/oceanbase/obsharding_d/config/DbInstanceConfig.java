/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */

public class DbInstanceConfig {

    private String instanceName;
    private String ip;
    private int port;
    private String url;
    private String user;
    private String password;
    private int readWeight;
    private String id;
    private boolean disabled;
    private boolean primary;
    private volatile int maxCon = -1;
    private volatile int minCon = -1;
    private volatile PoolConfig poolConfig;
    private boolean usingDecrypt;
    private DataBaseType dataBaseType;
    private String dbDistrict;
    private String dbDataCenter;

}
