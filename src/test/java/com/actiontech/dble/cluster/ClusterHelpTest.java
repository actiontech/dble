/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClusterHelpTest {

    @Test
    public void testShardingXml() throws Exception {
        String READ_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "sharding_template.xml";
        String originXml = loadOriginXml(ClusterPathUtil.LOCAL_WRITE_PATH + "sharding.dtd", READ_PATH);
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Shardings.class);
        xmlProcess.initJaxbClass();
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        Gson gson = gsonBuilder.create();
        String jsonContent = ClusterLogic.parseShardingXmlFileToJson(xmlProcess, gson, READ_PATH);
        Shardings newShardingBean = ClusterLogic.parseShardingJsonToBean(gson, jsonContent);
        ClusterLogic.writeMapFileAddFunction(newShardingBean.getFunction());
        String newXml = xmlProcess.baseParseToString(newShardingBean, "sharding");
        Assert.assertEquals(originXml.length(), newXml.length());
    }

    @Test
    public void testDXml() throws Exception {
        String READ_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "db_template.xml";
        String originXml = loadOriginXml(ClusterPathUtil.LOCAL_WRITE_PATH + "db.dtd", READ_PATH);
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(DbGroups.class);
        xmlProcess.initJaxbClass();
        Gson gson = new Gson();
        String jsonContent = ClusterLogic.parseDbGroupXmlFileToJson(xmlProcess, gson, READ_PATH);
        DbGroups newDbGroupsBean = ClusterLogic.parseDbGroupsJsonToBean(gson, jsonContent);
        String newXml = xmlProcess.baseParseToString(newDbGroupsBean, "db");
        Assert.assertEquals(originXml.length(), newXml.length());
    }


    @Test
    public void testUserXml() throws Exception {
        String READ_PATH = ClusterPathUtil.LOCAL_WRITE_PATH + "user_template.xml";
        String originXml = loadOriginXml(ClusterPathUtil.LOCAL_WRITE_PATH + "user.dtd", READ_PATH);
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Users.class);
        xmlProcess.initJaxbClass();
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        Gson gson = gsonBuilder.create();
        String jsonContent = ClusterLogic.parseUserXmlFileToJson(xmlProcess, gson, READ_PATH);
        Users newShardingBean = ClusterLogic.parseUserJsonToBean(gson, jsonContent);
        String newXml = xmlProcess.baseParseToString(newShardingBean, "user");
        Assert.assertEquals(originXml.length(), newXml.length());
    }


    private String loadOriginXml(String dtdFile, String xmlFile) throws Exception {
        InputStream dtd = ResourceUtil.getResourceAsStreamFromRoot(dtdFile);
        InputStream xmlStream = ResourceUtil.getResourceAsStreamFromRoot(xmlFile);
        Document root = ConfigUtil.getDocument(dtd, xmlStream);
        StringWriter sw = new StringWriter();
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.transform(new DOMSource(root), new StreamResult(sw));
        String xml = sw.toString();
        xml = xml.replace("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>", "");
        if (xml.charAt(xml.length() - 1) == '\n') {
            xml = xml.substring(0, xml.length() - 1);
        }
        String regexPattern = "( )*<!-[\\s\\S]*?-->";
        Pattern pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(xml);
        xml = matcher.replaceAll("");
        return xml.replaceAll("((\r\n)|\n)[\\s\t ]*(\\1)+", "$1").replaceAll("^((\r\n)|\n)", "").replaceAll("(\r)", "");
    }
}
