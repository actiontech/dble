package com.oceanbase.obsharding_d.config;

/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */
public class SingleDbGroupUserConfig extends ServerUserConfig {
    protected String dbGroup;
}
