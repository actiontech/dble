/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Mycat conf file related Handler
 *
 * @author wuzh
 */
public final class ConfFileHandler {
    private ConfFileHandler() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfFileHandler.class);
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final String UPLOAD_CMD = "FILE @@UPLOAD";

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DATA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void handle(String stmt, ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);
        // write rows
        byte packetId = EOF.getPacketId();
        String theStmt = stmt.toUpperCase().trim();
        PackageBufINf bufInf;
        if (theStmt.equals("FILE @@LIST")) {
            bufInf = listConfigFiles(service, buffer, packetId);
        } else if (theStmt.startsWith("FILE @@SHOW")) {
            int index = stmt.lastIndexOf(' ');
            String fileName = stmt.substring(index + 1);
            bufInf = showConfigFile(service, buffer, packetId, fileName);
        } else if (theStmt.startsWith(UPLOAD_CMD)) {
            int index = stmt.indexOf(' ', UPLOAD_CMD.length());
            int index2 = stmt.indexOf(' ', index + 1);
            if (index <= 0 || index2 <= 0 || index + 1 > stmt.length() || index2 + 1 > stmt.length()) {
                bufInf = showInfo(service, buffer, packetId, "Invald param ,usage  ");
            }
            String fileName = stmt.substring(index + 1, index2);
            String content = stmt.substring(index2 + 1).trim();
            bufInf = upLoadConfigFile(service, buffer, packetId, fileName, content);
        } else {
            bufInf = showInfo(service, buffer, packetId, "Invald command ");
        }

        packetId = bufInf.getPacketId();
        buffer = bufInf.getBuffer();

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static void checkXMLFile(String xmlFileName, byte[] data)
            throws ParserConfigurationException, SAXException, IOException {
        InputStream dtdStream = new ByteArrayInputStream(new byte[0]);
        File confDir = new File(SystemConfig.getInstance().getHomePath(), "conf");
        switch (xmlFileName) {
            case ConfigFileName.SHARDING_XML:
                dtdStream = ResourceUtil.getResourceAsStream("/sharding.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "sharding.dtd")));
                }

                break;
            case ConfigFileName.DB_XML:
                dtdStream = ResourceUtil.getResourceAsStream("/db.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "db.dtd")));
                }
                break;
            case ConfigFileName.USER_XML:
                dtdStream = ResourceUtil.getResourceAsStream("/user.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "user.dtd")));
                }
                break;
            default:
                break;
        }
        ConfigUtil.getDocument(dtdStream, new ByteArrayInputStream(data));
    }

    private static byte[] readFileByBytes(File fileName) {
        InputStream in = null;
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            byte[] tempBytes = new byte[100];
            int byteRead;
            in = new FileInputStream(fileName);
            while ((byteRead = in.read(tempBytes)) != -1) {
                outStream.write(tempBytes, 0, byteRead);
            }
        } catch (Exception e1) {
            LOGGER.info("readFileByBytesError", e1);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    LOGGER.info("readFileByBytesError", e1);
                }
            }
        }
        return outStream.toByteArray();
    }

    private static PackageBufINf upLoadConfigFile(ManagerService c,
                                                  ByteBuffer buffer, byte packetId, String fileName, String content) {
        LOGGER.info("Upload Daas Config file " + fileName + " ,content:" + content);
        String tempFileName = System.currentTimeMillis() + "_" + fileName;
        File tempFile = new File(SystemConfig.getInstance().getHomePath(), "conf" + File.separator + tempFileName);
        BufferedOutputStream buff = null;
        boolean suc = false;
        try {
            byte[] fileData = content.getBytes("UTF-8");
            if (fileName.endsWith(".xml")) {
                checkXMLFile(fileName, fileData);
            }
            buff = new BufferedOutputStream(new FileOutputStream(tempFile));
            buff.write(fileData);
            buff.flush();

        } catch (Exception e) {
            LOGGER.warn("write file err " + e);
            return showInfo(c, buffer, packetId, "write file err " + e);

        } finally {
            if (buff != null) {
                try {
                    buff.close();
                    suc = true;
                } catch (IOException e) {
                    LOGGER.warn("save config file err " + e);
                }
            }
        }
        if (suc) {
            // if succcess
            File oldFile = new File(SystemConfig.getInstance().getHomePath(), "conf" + File.separator + fileName);
            if (oldFile.exists()) {
                File backUP = new File(SystemConfig.getInstance().getHomePath(), "conf" +
                        File.separator + fileName + "_" +
                        System.currentTimeMillis() + "_auto");
                if (!oldFile.renameTo(backUP)) {
                    String msg = "rename old file failed";
                    LOGGER.warn(msg + " for upload file " + oldFile.getAbsolutePath());
                    return showInfo(c, buffer, packetId, msg);
                }
            }
            File dst = new File(SystemConfig.getInstance().getHomePath(), "conf" + File.separator + fileName);
            if (!tempFile.renameTo(dst)) {
                String msg = "rename file failed";
                LOGGER.warn(msg + " for upload file " + tempFile.getAbsolutePath());
                return showInfo(c, buffer, packetId, msg);
            }
            return showInfo(c, buffer, packetId, "SUCCESS SAVED FILE:" + fileName);
        } else {
            return showInfo(c, buffer, packetId, "UPLOAD ERROR OCCURD:" + fileName);
        }
    }

    private static PackageBufINf showInfo(ManagerService c,
                                          ByteBuffer buffer, byte packetId, String string) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(string, c.getCharset().getResults()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        PackageBufINf bufINf = new PackageBufINf();
        bufINf.setPacketId(packetId);
        bufINf.setBuffer(buffer);
        return bufINf;
    }

    private static PackageBufINf showConfigFile(ManagerService c,
                                                ByteBuffer buffer, byte packetId, String fileName) {
        File file = new File(SystemConfig.getInstance().getHomePath(), "conf" + File.separator + fileName);
        BufferedReader br = null;
        PackageBufINf bufINf = new PackageBufINf();
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(line, c.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
            bufINf.setBuffer(buffer);
            bufINf.setPacketId(packetId);
            return bufINf;

        } catch (Exception e) {
            LOGGER.info("showConfigFileError", e);
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(e.toString(), c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
            bufINf.setBuffer(buffer);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.info("showConfigFileError", e);
                }
            }

        }
        bufINf.setPacketId(packetId);
        return bufINf;
    }

    private static PackageBufINf listConfigFiles(ManagerService c,
                                                 ByteBuffer buffer, byte packetId) {
        PackageBufINf bufINf = new PackageBufINf();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            int i = 1;
            File[] file = new File(SystemConfig.getInstance().getHomePath(), "conf").listFiles();
            if (file != null) {
                for (File f : file) {
                    if (f.isFile()) {
                        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                        row.add(StringUtil.encode(
                                (i++) + " : " + f.getName() + "  time:" +
                                        df.format(new Date(f.lastModified())),
                                c.getCharset().getResults()));
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, c, true);
                    }
                }
            }
            bufINf.setBuffer(buffer);
            bufINf.setPacketId(packetId);
            return bufINf;

        } catch (Exception e) {
            LOGGER.info("listConfigFilesError", e);
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(e.toString(), c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
            bufINf.setBuffer(buffer);
        }
        bufINf.setPacketId(packetId);
        return bufINf;
    }
}
