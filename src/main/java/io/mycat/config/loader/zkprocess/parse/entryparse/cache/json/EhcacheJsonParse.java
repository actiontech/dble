package io.mycat.config.loader.zkprocess.parse.entryparse.cache.json;

import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * EhcacheJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
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
