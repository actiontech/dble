package io.mycat.config.loader.zkprocess.parse.entryparse.rule.xml;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.entity.Rules;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;

/**
 * RuleParseXmlImpl
 *
 *
 * author:liujun
 * Created:2016/9/16
 *
 *
 *
 *
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
