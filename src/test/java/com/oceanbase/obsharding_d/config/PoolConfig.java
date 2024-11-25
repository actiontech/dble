/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.*;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class PoolConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolConfig.class);
    private static final String WARNING_FORMAT = "Property [ %s ] '%d' in db.xml is illegal, use the default value %d replaced";

    public static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    public static final long CON_HEARTBEAT_TIMEOUT = MILLISECONDS.toMillis(20);
    public static final long DEFAULT_IDLE_TIMEOUT = MINUTES.toMillis(10);
    public static final long HOUSEKEEPING_PERIOD_MS = SECONDS.toMillis(30);
    public static final long DEFAULT_HEARTBEAT_PERIOD = SECONDS.toMillis(10);
    public static final long DEFAULT_SHUTDOWN_TIMEOUT = SECONDS.toMillis(10);

    private volatile long connectionTimeout = CONNECTION_TIMEOUT;
    private volatile long connectionHeartbeatTimeout = CON_HEARTBEAT_TIMEOUT;
    private volatile boolean testOnCreate = false;
    private volatile boolean testOnBorrow = false;
    private volatile boolean testOnReturn = false;
    private volatile boolean testWhileIdle = false;
    private volatile long timeBetweenEvictionRunsMillis = HOUSEKEEPING_PERIOD_MS;
    private volatile long evictorShutdownTimeoutMillis = DEFAULT_SHUTDOWN_TIMEOUT;
    private volatile long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private volatile long heartbeatPeriodMillis = DEFAULT_HEARTBEAT_PERIOD;

    private volatile int flowHighLevel = SystemConfig.FLOW_CONTROL_HIGH_LEVEL;
    private volatile int flowLowLevel = SystemConfig.FLOW_CONTROL_LOW_LEVEL;

}
