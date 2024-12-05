/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class ShardingNode {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingNode.class);

    protected String name;
    private String dbGroupName;
    protected String database;
    protected volatile PhysicalDbGroup dbGroup;
    private volatile boolean isSchemaExists = false;
}
