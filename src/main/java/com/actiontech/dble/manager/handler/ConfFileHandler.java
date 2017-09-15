/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
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

    public static void handle(String stmt, ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);
        // write rows
        byte packetId = EOF.getPacketId();
        String theStmt = stmt.toUpperCase().trim();
        PackageBufINf bufInf;
        if (theStmt.equals("FILE @@LIST")) {
            bufInf = listConfigFiles(c, buffer, packetId);
        } else if (theStmt.startsWith("FILE @@SHOW")) {
            int index = stmt.lastIndexOf(' ');
            String fileName = stmt.substring(index + 1);
            bufInf = showConfigFile(c, buffer, packetId, fileName);
        } else if (theStmt.startsWith(UPLOAD_CMD)) {
            int index = stmt.indexOf(' ', UPLOAD_CMD.length());
            int index2 = stmt.indexOf(' ', index + 1);
            if (index <= 0 || index2 <= 0 || index + 1 > stmt.length() || index2 + 1 > stmt.length()) {
                bufInf = showInfo(c, buffer, packetId, "Invald param ,usage  ");
            }
            String fileName = stmt.substring(index + 1, index2);
            String content = stmt.substring(index2 + 1).trim();
            bufInf = upLoadConfigFile(c, buffer, packetId, fileName, content);
        } else {
            bufInf = showInfo(c, buffer, packetId, "Invald command ");
        }

        packetId = bufInf.getPacketId();
        buffer = bufInf.getBuffer();

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static void checkXMLFile(String xmlFileName, byte[] data)
            throws ParserConfigurationException, SAXException, IOException {
        InputStream dtdStream = new ByteArrayInputStream(new byte[0]);
        File confDir = new File(SystemConfig.getHomePath(), "conf");
        switch (xmlFileName) {
            case "schema.xml":
                dtdStream = ResourceUtil.getResourceAsStream("/schema.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "schema.dtd")));
                }

                break;
            case "server.xml":
                dtdStream = ResourceUtil.getResourceAsStream("/server.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "server.dtd")));
                }
                break;
            case "rule.xml":
                dtdStream = ResourceUtil.getResourceAsStream("/rule.dtd");
                if (dtdStream == null) {
                    dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
                            confDir, "rule.dtd")));
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
            byte[] tempbytes = new byte[100];
            int byteread;
            in = new FileInputStream(fileName);
            while ((byteread = in.read(tempbytes)) != -1) {
                outStream.write(tempbytes, 0, byteread);
            }
        } catch (Exception e1) {
            LOGGER.error("readFileByBytesError", e1);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    LOGGER.error("readFileByBytesError", e1);
                }
            }
        }
        return outStream.toByteArray();
    }

    private static PackageBufINf upLoadConfigFile(ManagerConnection c,
                                                  ByteBuffer buffer, byte packetId, String fileName, String content) {
        LOGGER.info("Upload Daas Config file " + fileName + " ,content:" + content);
        String tempFileName = System.currentTimeMillis() + "_" + fileName;
        File tempFile = new File(SystemConfig.getHomePath(), "conf" + File.separator + tempFileName);
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
            File oldFile = new File(SystemConfig.getHomePath(), "conf" + File.separator + fileName);
            if (oldFile.exists()) {
                File backUP = new File(SystemConfig.getHomePath(), "conf" +
                        File.separator + fileName + "_" +
                        System.currentTimeMillis() + "_auto");
                if (!oldFile.renameTo(backUP)) {
                    String msg = "rename old file failed";
                    LOGGER.warn(msg + " for upload file " + oldFile.getAbsolutePath());
                    return showInfo(c, buffer, packetId, msg);
                }
            }
            File dest = new File(SystemConfig.getHomePath(), "conf" + File.separator + fileName);
            if (!tempFile.renameTo(dest)) {
                String msg = "rename file failed";
                LOGGER.warn(msg + " for upload file " + tempFile.getAbsolutePath());
                return showInfo(c, buffer, packetId, msg);
            }
            return showInfo(c, buffer, packetId, "SUCCESS SAVED FILE:" + fileName);
        } else {
            return showInfo(c, buffer, packetId, "UPLOAD ERROR OCCURD:" + fileName);
        }
    }

    private static PackageBufINf showInfo(ManagerConnection c,
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

    private static PackageBufINf showConfigFile(ManagerConnection c,
                                                ByteBuffer buffer, byte packetId, String fileName) {
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + fileName);
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
            LOGGER.error("showConfigFileError", e);
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
                    LOGGER.error("showConfigFileError", e);
                }
            }

        }
        bufINf.setPacketId(packetId);
        return bufINf;
    }

    private static PackageBufINf listConfigFiles(ManagerConnection c,
                                                 ByteBuffer buffer, byte packetId) {
        PackageBufINf bufINf = new PackageBufINf();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            int i = 1;
            File[] file = new File(SystemConfig.getHomePath(), "conf").listFiles();
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
            LOGGER.error("listConfigFilesError", e);
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
