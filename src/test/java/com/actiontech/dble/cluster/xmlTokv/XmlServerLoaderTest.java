package com.actiontech.dble.cluster.xmlTokv;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.entity.Server;
import com.actiontech.dble.config.loader.zkprocess.entity.server.User;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.json.UserJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.xml.ServerParseXmlImpl;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class XmlServerLoaderTest {
    private static final String WRITEPATH = "server.xml";
    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    @Test
    public void testUser() throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        ParseXmlServiceInf<Server> parseServerXMl = new ServerParseXmlImpl(xmlProcess);
        xmlProcess.initJaxbClass();

        Server xml2Server = parseServerXMl.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject serverJson = new JSONObject();
        serverJson.put(ClusterPathUtil.USER, xml2Server.getUser());
        JSONObject jsonObj = JSONObject.parseObject(serverJson.toJSONString());

        List<User> user = parseJsonUser.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.USER).toJSONString());
        Assert.assertEquals(2, user.get(0).getPrivileges().getSchema().get(0).getTable().size());
    }
}
