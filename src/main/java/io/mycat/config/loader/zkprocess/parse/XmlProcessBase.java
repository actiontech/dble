package io.mycat.config.loader.zkprocess.parse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.Versions;
import io.mycat.util.ResourceUtil;

/**
 * XmlProcessBase
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class XmlProcessBase {


    private static final Logger LOGGER = LoggerFactory.getLogger(XmlProcessBase.class);

    private JAXBContext jaxContext;

    private Unmarshaller unmarshaller;

    @SuppressWarnings("rawtypes")
    private List<Class> parseXmlClass = new ArrayList<>();

    /**
     *
     * @param parseClass
     * @Created 2016/9/15
     */
    @SuppressWarnings("rawtypes")
    public void addParseClass(Class parseClass) {
        this.parseXmlClass.add(parseClass);
    }

    /**
     * initJaxbClass
     *
     * @throws JAXBException
     * @Created 2016/9/15
     */
    @SuppressWarnings("rawtypes")
    public void initJaxbClass() throws JAXBException {

        Class[] classArray = new Class[parseXmlClass.size()];
        parseXmlClass.toArray(classArray);

        try {
            this.jaxContext = JAXBContext.newInstance(classArray, Collections.<String, Object>emptyMap());
        } catch (JAXBException e) {
            LOGGER.error("ZookeeperProcessListen initJaxbClass  error:Exception info:", e);
            throw e;
        }

        // Deserialization
        unmarshaller = jaxContext.createUnmarshaller();
    }

    /**
     *baseParseAndWriteToXml
     *
     * @param user
     * @param inputPath
     * @param name
     * @Created 2016/9/15
     */
    public void baseParseAndWriteToXml(Object user, String inputPath, String name) throws IOException {
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE " + Versions.ROOT_PREFIX + ":%1$s SYSTEM \"%1$s.dtd\">", name));
            }

            Path path = Paths.get(inputPath);

            OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(user, out);

        } catch (JAXBException | IOException e) {
            LOGGER.error("ZookeeperProcessListen parseToXml  error:Exception info:", e);
        }
    }

    /**
     * baseParseAndWriteToXml
     *
     * @param user
     * @param inputPath
     * @param name
     * @Created 2016/9/15
     */
    @SuppressWarnings("restriction")
    public void baseParseAndWriteToXml(Object user, String inputPath, String name, Map<String, Object> map)
            throws IOException {
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

            OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(user, out);

        } catch (JAXBException | IOException e) {
            LOGGER.error("ZookeeperProcessListen parseToXml  error:Exception info:", e);
        }
    }

    /**
     * baseParseXmlToBean
     *
     * @param fileName
     * @return
     * @throws JAXBException
     * @throws XMLStreamException
     * @Created 2016/9/16
     */
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

}
