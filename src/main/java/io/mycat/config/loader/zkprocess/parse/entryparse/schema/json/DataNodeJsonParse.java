package io.mycat.config.loader.zkprocess.parse.entryparse.schema.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * DataNodeJsonParse
 *
 *
 * author:liujun
 * Created:2016/9/17
 *
 *
 *
 *
 */
public class DataNodeJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<DataNode>> {

    @Override
    public String parseBeanToJson(List<DataNode> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<DataNode> parseJsonToBean(String json) {
        Type parseType = new TypeToken<List<DataNode>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
