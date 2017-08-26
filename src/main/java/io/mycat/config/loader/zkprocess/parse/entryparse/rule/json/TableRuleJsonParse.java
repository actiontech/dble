package io.mycat.config.loader.zkprocess.parse.entryparse.rule.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * TableRuleJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class TableRuleJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<TableRule>> {

    @Override
    public String parseBeanToJson(List<TableRule> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<TableRule> parseJsonToBean(String json) {

        Type parseType = new TypeToken<List<TableRule>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
