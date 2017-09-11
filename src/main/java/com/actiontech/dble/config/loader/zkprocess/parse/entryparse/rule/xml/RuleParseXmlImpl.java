/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.rule.xml;

import com.actiontech.dble.config.loader.zkprocess.entity.Rules;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * RuleParseXmlImpl
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/16
 */
public class RuleParseXmlImpl implements ParseXmlServiceInf<Rules> {


    private static final Logger LOGGER = LoggerFactory.getLogger(RuleParseXmlImpl.class);

    private XmlProcessBase parseBean;

    public RuleParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        parseBean.addParseClass(Rules.class);
    }

    @Override
    public Rules parseXmlToBean(String path) {

        Rules schema = null;

        try {
            schema = (Rules) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            LOGGER.error("RulesParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            LOGGER.error("RulesParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return schema;
    }

    @Override
    public void parseToXmlWrite(Rules data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("RulesParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
