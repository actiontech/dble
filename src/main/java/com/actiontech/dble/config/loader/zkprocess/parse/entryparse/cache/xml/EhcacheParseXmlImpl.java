/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.xml;

import com.actiontech.dble.config.loader.zkprocess.entity.cache.Ehcache;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * EhcacheParseXmlImpl
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/16
 */
public class EhcacheParseXmlImpl implements ParseXmlServiceInf<Ehcache> {


    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheParseXmlImpl.class);

    private XmlProcessBase parseBean;

    public EhcacheParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        // add transform xml object
        parseBean.addParseClass(Ehcache.class);
    }

    @Override
    public Ehcache parseXmlToBean(String path) {

        Ehcache schema = null;

        try {
            schema = (Ehcache) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            LOGGER.error("EhcacheParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            LOGGER.error("EhcacheParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return schema;
    }

    @Override
    public void parseToXmlWrite(Ehcache data, String outputFile, String dataName) {
        try {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put(Marshaller.JAXB_NO_NAMESPACE_SCHEMA_LOCATION, "ehcache.xsd");

            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName, paramMap);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("EhcacheParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}


