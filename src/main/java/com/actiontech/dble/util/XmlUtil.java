/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.util;

import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;

public final class XmlUtil {

    private XmlUtil() {
    }

    public static void saveDocument(Document document, String xmlFilePath, String defaultDtdName, boolean standalone) throws TransformerException {
        document.setXmlStandalone(standalone);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        Source xmlSource = new DOMSource(document);
        Result outputTarget = new StreamResult(new File(xmlFilePath));
        DocumentType doctype = document.getImplementation().createDocumentType("doctype", "", defaultDtdName);
        transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, doctype.getSystemId());
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.transform(xmlSource, outputTarget);
    }

}
