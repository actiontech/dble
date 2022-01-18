/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

import java.util.EnumSet;

import static com.actiontech.dble.net.service.WriteFlag.*;

/**
 * @author dcy
 * Create Date: 2021-05-19
 */
public interface WriteFlags {
    EnumSet<WriteFlag> QUERY_END = EnumSet.of(END_OF_QUERY, FLUSH);
    EnumSet<WriteFlag> SESSION_END = EnumSet.of(END_OF_SESSION, FLUSH);
    /**
     * unfinished
     */
    EnumSet<WriteFlag> PART = EnumSet.noneOf(WriteFlag.class);


    void test();
}
