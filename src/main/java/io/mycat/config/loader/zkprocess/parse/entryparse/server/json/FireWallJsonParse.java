package io.mycat.config.loader.zkprocess.parse.entryparse.server.json;

import io.mycat.config.loader.zkprocess.entity.server.FireWall;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

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
