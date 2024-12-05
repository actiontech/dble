/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;
/**
 * requires attention:
 * New fields need to consider equals and copyBaseInfo methods
 * http://10.186.18.11/jira/browse/OBsharding-D0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
 */

import java.util.regex.Pattern;

public class DbGroupConfig {
    private static final Pattern HP_PATTERN_SHOW_SLAVE_STATUS = Pattern.compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern HP_PATTERN_READ_ONLY = Pattern.compile("\\s*select\\s+@@read_only\\s*", Pattern.CASE_INSENSITIVE);
    private String name;
    private int rwSplitMode = PhysicalDbGroup.RW_SPLIT_OFF;
    private DbInstanceConfig writeInstanceConfig;
    private DbInstanceConfig[] readInstanceConfigs;
    private String heartbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isSelectReadOnlySql = false;
    private int delayThreshold;

    private int heartbeatTimeout = 0;
    private int errorRetryCount = 1;
    private int keepAlive = 60;
    private boolean disableHA;
}
