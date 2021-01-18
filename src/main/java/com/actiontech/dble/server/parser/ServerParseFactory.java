/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.parser;

/**
 * @author dcy
 * Create Date: 2021-01-12
 */
public final class ServerParseFactory {
    private ServerParseFactory() {
    }


    public static ShardingServerParse getShardingParser() {
        return new ShardingServerParse();
    }

    public static RwSplitServerParse getRwSplitParser() {
        return new RwSplitServerParse();
    }


}
