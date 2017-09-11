/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.json;

import com.actiontech.dble.config.loader.zkprocess.entity.cache.Ehcache;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * EhcacheJsonParse
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/17
 */
public class EhcacheJsonParse extends JsonProcessBase implements ParseJsonServiceInf<Ehcache> {

    @Override
    public String parseBeanToJson(Ehcache t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public Ehcache parseJsonToBean(String json) {
        return this.toBeanformJson(json, Ehcache.class);
    }

}
