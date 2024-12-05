/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.config.model.user.UserPrivilegesConfig;

import java.util.Set;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class ShardingUserConfig extends ServerUserConfig {
    private boolean readOnly;
    private Set<String> schemas;
    private UserPrivilegesConfig privilegesConfig;
}
