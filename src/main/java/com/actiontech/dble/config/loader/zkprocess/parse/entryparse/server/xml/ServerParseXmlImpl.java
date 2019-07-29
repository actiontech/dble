/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse.entryparse.server.xml;

import com.actiontech.dble.config.loader.zkprocess.entity.Server;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * ServerParseXmlImpl
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/16
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
            LOGGER.warn("ServerParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            LOGGER.warn("ServerParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return server;
    }

    @Override
    public void parseToXmlWrite(Server data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            LOGGER.warn("ServerParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
