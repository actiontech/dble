/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.parse;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    private List<Class> parseXmlClass = new ArrayList<>();


    @SuppressWarnings("rawtypes")
    public void addParseClass(Class parseClass) {
        this.parseXmlClass.add(parseClass);
    }


    @SuppressWarnings("rawtypes")
    public void initJaxbClass() throws JAXBException {

        Class[] classArray = new Class[parseXmlClass.size()];
        parseXmlClass.toArray(classArray);

        this.jaxContext = JAXBContext.newInstance(classArray, Collections.<String, Object>emptyMap());

        // Deserialization
        unmarshaller = jaxContext.createUnmarshaller();
    }


    public void baseParseAndWriteToXml(Object obj, String inputPath, String name) throws IOException {
        OutputStream out = null;
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            }

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

    @SuppressWarnings("restriction")
    public void baseParseAndWriteToXml(Object obj, String inputPath, String name, Map<String, Object> map)
            throws IOException {
        OutputStream out = null;
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            }

            if (null != map && !map.isEmpty()) {
                for (Entry<String, Object> entry : map.entrySet()) {
                    marshaller.setProperty(entry.getKey(), entry.getValue());
                }
            }

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


    public void writeObjToXml(Object obj, String inputPath, String name) {
        if (null == obj || StringUtil.isEmpty(inputPath) || StringUtil.isEmpty(name)) {
            return;
        }
        try {
            //backup
            File tempFile = new File(inputPath + ".tmp");
            File file = new File(inputPath);
            FileUtils.copy(file, tempFile);

            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                    String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            Result outputTarget = new StreamResult(new File(inputPath));

            marshaller.marshal(obj, outputTarget);

        } catch (JAXBException | IOException e) {
            LOGGER.error("parseToXml  error:Exception info:", e);
        }
    }

    public String baseParseToString(Object obj, String name) {
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            }
            StringWriter sw = new StringWriter();
            marshaller.marshal(obj, sw);

            return sw.toString();

        } catch (JAXBException e) {
            LOGGER.error("parseToXml  error:Exception info:", e);
            return null;
        }
    }


    public Object baseParseXmlToBean(String fileName) throws JAXBException, XMLStreamException {
        InputStream inputStream = ResourceUtil.getResourceAsStreamFromRoot(fileName);
        if (inputStream != null) {
            XMLInputFactory xif = XMLInputFactory.newFactory();
            xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            XMLStreamReader xmlRead = xif.createXMLStreamReader(new StreamSource(inputStream));
            return unmarshaller.unmarshal(xmlRead);
        }

        return null;
    }


    public void safeParseWriteToXml(Object obj, String inputPath, String name) throws IOException {
        OutputStream out = null;
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            }

            Path path = Paths.get(inputPath + ".dble.safe");

            out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(obj, out);

            //writeDirectly out xml.dble.safe file finish
            // rename it into sharding.xml
            File fromFile = new File(inputPath + ".dble.safe");
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
