/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

/**
 * DEF Alarms Keyword
 *
 * @author mycat
 */
public final class Alarms {
    private Alarms() {
    }

    /**
     * default
     **/
    public static final String DEFAULT = "#!Server#";

    /**
     * switch the data source
     **/
    public static final String DATANODE_SWITCH = "#!DN_SWITCH#";

    /**
     * ATTACK the FIREWALL
     **/
    public static final String FIREWALL_ATTACK = "#!QT_ATTACK#";

    /**
     * DML
     **/
    public static final String DML_ATTACK = "#!DML_ATTACK#";

}
