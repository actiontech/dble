/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.storage;

/**
 * Created by zagnix on 2016/6/3.
 */
public class TempDataNodeId extends ConnectionId {

    private String uuid;

    public TempDataNodeId(String uuid) {
        super();
        this.name = uuid;
        this.uuid = uuid;
    }

    @Override
    public String getBlockName() {
        return "temp_local_" + uuid;
    }
}
