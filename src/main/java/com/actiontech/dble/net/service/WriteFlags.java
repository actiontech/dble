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
    // query sql return OKPacket or EOFRowPacket, etc (The end package for the Query protocol)
    // multi-query return last of the result
    EnumSet<WriteFlag> QUERY_END = EnumSet.of(END_OF_QUERY, FLUSH);

    // return ErrorPacket
    EnumSet<WriteFlag> SESSION_END = EnumSet.of(END_OF_SESSION, FLUSH);

    // multi-query return part of the result
    EnumSet<WriteFlag> MULTI_QUERY_PART = EnumSet.of(PARK_OF_MULTI_QUERY, FLUSH);

    // unfinished (such as: row、larger result set)
    EnumSet<WriteFlag> PART = EnumSet.noneOf(WriteFlag.class);


    void test();
}
