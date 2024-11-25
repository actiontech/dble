/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.backend.datasource.LoadBalancer;
import com.oceanbase.obsharding_d.backend.datasource.LocalReadLoadBalancer;
import com.oceanbase.obsharding_d.backend.datasource.RandomLoadBalancer;
import com.oceanbase.obsharding_d.net.Session;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class PhysicalDbGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    public static final String JSON_NAME = "dbGroup";
    public static final String JSON_LIST = "dbInstance";
    // rw split
    public static final int RW_SPLIT_OFF = 0;
    public static final int RW_SPLIT_ALL = 2;
    public static final int RW_SPLIT_ALL_SLAVES_MAY_MASTER = 3;
    private List<PhysicalDbInstance> writeInstanceList;

    private String groupName;
    private DbGroupConfig dbGroupConfig;
    private volatile PhysicalDbInstance writeDbInstance;
    private Map<String, PhysicalDbInstance> allSourceMap = new HashMap<>();

    private int rwSplitMode;
    protected List<String> schemas = Lists.newArrayList();
    private final LoadBalancer loadBalancer = new RandomLoadBalancer();
    private final LocalReadLoadBalancer localReadLoadBalancer = new LocalReadLoadBalancer();
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();

    private boolean shardingUseless = true;
    private boolean rwSplitUseless = true;
    private boolean analysisUseless = true;
    private Set<Session> rwSplitSessionSet = Sets.newConcurrentHashSet();
    private volatile Integer state = Integer.valueOf(INITIAL);


    public static final int STATE_DELETING = 2;
    public static final int STATE_ABANDONED = 1;
    public static final int INITIAL = 0;
}
