/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.parse;

import com.oceanbase.obsharding_d.util.ResourceUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * XmlProcessBase
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class XmlProcessBase {


    private static final Logger LOGGER = LoggerFactory.getLogger(XmlProcessBase.class);

    private JAXBContext jaxContext;

    private Unmarshaller unmarshaller;

    @SuppressWarnings("rawtypes")
    private final List<Class> parseXmlClass = new ArrayList<>();


    @SuppressWarnings("rawtypes")
    public void addParseClass(Class parseClass) {
        this.parseXmlClass.add(parseClass);
    }


    @SuppressWarnings("rawtypes")
    public void initJaxbClass() throws JAXBException {

        Class[] classArray = new Class[parseXmlClass.size()];
        parseXmlClass.toArray(classArray);

        this.jaxContext = JAXBContext.newInstance(classArray, Collections.emptyMap());

        // Deserialization
        unmarshaller = jaxContext.createUnmarshaller();
    }


    public void baseParseAndWriteToXml(Object obj, String inputPath, String name) throws IOException {
        OutputStream out = null;
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            Path path = Paths.get(inputPath);

            out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(obj, out);

        } catch (JAXBException | IOException e) {
            LOGGER.error("parseToXml  error:Exception info:", e);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public Object baseParseXmlToBean(String xmlPath, String xsdPath) throws JAXBException, SAXException, XMLStreamException {
        if (!StringUtil.isBlank(xmlPath) && !StringUtil.isBlank(xsdPath)) {
            InputStream inputStream = ResourceUtil.getResourceAsStreamFromRoot(xmlPath);
            //check
            InputStream xsdStream = ResourceUtil.getResourceAsStreamFromRoot(xsdPath);
            return baseParseXmlToBean(inputStream, xsdStream);
        }
        return null;
    }

    public Object baseParseXmlToBean(InputStream xmlStream, InputStream xsdStream) throws JAXBException, SAXException, XMLStreamException {
        if (null != xsdStream && null != xmlStream) {
            //check
            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            sf.setResourceResolver((type, namespaceURI, publicId, systemId, baseURI) -> {
                InputStream resourceAsStream = ResourceUtil.getResourceAsStreamFromRoot(systemId);
                return new Input(publicId, systemId, resourceAsStream);
            });
            Schema schema = sf.newSchema(new StreamSource(xsdStream));
            unmarshaller.setSchema(schema);
            XMLInputFactory xif = XMLInputFactory.newFactory();
            xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            XMLStreamReader xmlRead = xif.createXMLStreamReader(new StreamSource(xmlStream));
            return unmarshaller.unmarshal(xmlRead);
        }
        return null;
    }

    public static void validate(InputStream xmlStream, InputStream xsdStream) throws SAXException, IOException {
        if (null != xsdStream && null != xmlStream) {
            //check
            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            sf.setResourceResolver((type, namespaceURI, publicId, systemId, baseURI) -> {
                InputStream resourceAsStream = ResourceUtil.getResourceAsStreamFromRoot(systemId);
                return new Input(publicId, systemId, resourceAsStream);
            });
            Schema schema = sf.newSchema(new StreamSource(xsdStream));
            Validator validator = schema.newValidator();
            validator.validate(new StreamSource(xmlStream));
        }
    }


    public void safeParseWriteToXml(Object obj, String inputPath, String name) throws IOException {
        OutputStream out = null;
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            Path path = Paths.get(inputPath + ".OBsharding-D.safe");

            out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(obj, out);

            //writeDirectly out xml.OBsharding-D.safe file finish
            // rename it into sharding.xml
            File fromFile = new File(inputPath + ".OBsharding-D.safe");
            File toFile = new File(inputPath);
            if (toFile.exists()) {
                toFile.delete();
            }
            if (!fromFile.renameTo(toFile)) {
                throw new IOException("rename file error for " + path.getFileName());
            }

        } catch (JAXBException | IOException e) {
            LOGGER.error("parseToXml  error:Exception info:", e);
            throw new IOException(e);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }


}
