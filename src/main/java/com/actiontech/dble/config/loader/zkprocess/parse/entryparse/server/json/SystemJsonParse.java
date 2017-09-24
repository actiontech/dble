/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json;

import com.actiontech.dble.config.loader.zkprocess.entity.server.System;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * SystemJsonParse
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/17
 */
public class SystemJsonParse extends JsonProcessBase implements ParseJsonServiceInf<System> {

    @Override
    public String parseBeanToJson(System t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public System parseJsonToBean(String json) {

        return this.toBeanformJson(json, System.class);
    }

}
