/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.pool.ConnectionPool;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/DBLE0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */

public abstract class PhysicalDbInstance implements ReadTimeStatusInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbInstance.class);

    private String name;
    private DbInstanceConfig config;
    private volatile boolean readInstance;

    private DbGroupConfig dbGroupConfig;
    private PhysicalDbGroup dbGroup;
    private AtomicBoolean disabled;
    private String dsVersion;
    private volatile boolean autocommitSynced;
    private volatile boolean isolationSynced;
    private volatile boolean testConnSuccess = false;
    private volatile boolean readOnly = false;
    private volatile boolean fakeNode = false;
    private final LongAdder readCount = new LongAdder();
    private final LongAdder writeCount = new LongAdder();

    private final AtomicBoolean isInitial = new AtomicBoolean(false);

    // connection pool
    private ConnectionPool connectionPool;
    protected MySQLHeartbeat heartbeat;
    private volatile boolean needSkipEvit = false;

}
