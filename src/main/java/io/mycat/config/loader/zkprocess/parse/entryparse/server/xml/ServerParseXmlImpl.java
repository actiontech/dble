package io.mycat.config.loader.zkprocess.parse.entryparse.server.xml;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.entity.Server;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;

/**
 * ServerParseXmlImpl
 *
 *
 * author:liujun
 * Created:2016/9/16
 *
 *
 *
 *
 */
public class ServerParseXmlImpl implements ParseXmlServiceInf<Server> {


    private static final Logger LOGGER = LoggerFactory.getLogger(ServerParseXmlImpl.class);

    private XmlProcessBase parseBean;

    public ServerParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        parseBean.addParseClass(Server.class);
    }

    @Override
    public Server parseXmlToBean(String path) {

        Server server = null;

        try {
            server = (Server) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            LOGGER.error("ServerParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            LOGGER.error("ServerParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return server;
    }

    @Override
    public void parseToXmlWrite(Server data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("ServerParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
