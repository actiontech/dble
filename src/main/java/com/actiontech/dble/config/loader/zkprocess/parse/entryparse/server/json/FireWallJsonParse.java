/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json;

import com.actiontech.dble.config.loader.zkprocess.entity.server.FireWall;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * Created by huqing.yan on 2017/6/16.
 */
public class FireWallJsonParse extends JsonProcessBase implements ParseJsonServiceInf<FireWall> {
    @Override
    public String parseBeanToJson(FireWall fireWall) {
        return this.toJsonFromBean(fireWall);
    }

    @Override
    public FireWall parseJsonToBean(String json) {
        return this.toBeanformJson(json, FireWall.class);
    }
}
