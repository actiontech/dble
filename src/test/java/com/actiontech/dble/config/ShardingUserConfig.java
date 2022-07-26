/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

import com.actiontech.dble.config.model.user.UserPrivilegesConfig;

import java.util.Set;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/DBLE0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class ShardingUserConfig extends ServerUserConfig {
    private boolean readOnly;
    private Set<String> schemas;
    private UserPrivilegesConfig privilegesConfig;
}
